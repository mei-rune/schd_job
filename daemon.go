package sched_job

import (
	"bytes"
	"encoding/json"
	"errors"
	"expvar"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/runner-mei/cron"
	"gopkg.in/fsnotify.v1"
)

var (
	listenAddress = flag.String("listen", ":37075", "the address of http")
	poll_interval = flag.Duration("poll_interval", 1*time.Minute, "the poll interval of db")
	is_print      = flag.Bool("print", false, "print search paths while config is not found")
	root_dir      = flag.String("root", ".", "the root directory")
	config_file   = flag.String("schd-config", "./<program_name>.conf", "the config file path")
	java_home     = flag.String("java_home", "", "the path of java, should auto search if it is empty")
	log_path      = flag.String("log_path", "", "the path of log, should auto search if it is empty")
)

func fileExists(nm string) bool {
	fs, e := os.Stat(nm)
	if nil != e {
		return false
	}
	return !fs.IsDir()
}

func dirExists(nm string) bool {
	fs, e := os.Stat(nm)
	if nil != e {
		return false
	}
	return fs.IsDir()
}

// func usage() {
// 	program := filepath.Base(os.Args[0])
// 	fmt.Fprint(os.Stderr, program, ` [options]
// Options:
// `)
// 	flag.PrintDefaults()
// }

func abs(s string) string {
	r, e := filepath.Abs(s)
	if nil != e {
		return s
	}
	return r
}

func Schedule(c *cron.Cron, id string, schedule cron.Schedule, cmd cron.Job) {
	c.Schedule(id, schedule, cmd)
}

func Main() {
	flag.Parse()
	if nil != flag.Args() && 0 != len(flag.Args()) {
		flag.Usage()
		return
	}

	if "." == *root_dir {
		*root_dir = abs(filepath.Dir(os.Args[0]))
		dirs := []string{abs(filepath.Dir(os.Args[0])), filepath.Join(abs(filepath.Dir(os.Args[0])), "..")}
		for _, s := range dirs {
			if dirExists(filepath.Join(s, "conf")) {
				*root_dir = s
				break
			}
		}
	} else {
		*root_dir = abs(*root_dir)
	}

	if !dirExists(*root_dir) {
		log.Println("root directory '" + *root_dir + "' is not exist.")
		return
	} else {
		log.Println("root directory is '" + *root_dir + "'.")
	}

	e := os.Chdir(*root_dir)
	if nil != e {
		log.Println("change current dir to \"" + *root_dir + "\"")
	}

	if 0 == len(*java_home) {
		flag.Set("java_home", search_java_home(*root_dir))
		log.Println("[warn] java is", *java_home)
	}

	arguments, e := loadConfig(*root_dir)
	if nil != e {
		log.Println(e)
		return
	}
	flag.Set("log_path", ensureLogPath(*root_dir, arguments))

	backend, e := newBackend(*db_drv, *db_url)
	if nil != e {
		fmt.Println("db_drv is", *db_drv)
		fmt.Println("db_url is", *db_url)
		log.Println(e)
		return
	}

	job_directories := []string{filepath.Join(*root_dir, "lib", "jobs"),
		filepath.Join(*root_dir, "data", "jobs")}
	jobs_from_dir, e := loadJobsFromDirectory(job_directories, arguments)
	if nil != e {
		log.Println(e)
	}
	jobs_from_db, e := loadJobsFromDB(backend, arguments)
	if nil != e {
		log.Println(e)
	}

	error_jobs := map[string]error{}
	cr := cron.New()
	if len(jobs_from_dir) > 0 {
		for _, job := range jobs_from_dir {
			sch, e := Parse(job.expression)
			if nil != e {
				error_jobs[job.name] = e
				log.Println("["+job.name+"] schedule failed,", e)
				continue
			}
			Schedule(cr, job.name, sch, job)
		}
	}

	if len(jobs_from_db) > 0 {
		for _, job := range jobs_from_db {
			sch, e := Parse(job.expression)
			if nil != e {
				e := errors.New("[" + job.name + "] schedule failed, " + e.Error())
				error_jobs[fmt.Sprint(job.id)] = e
				log.Println(e)
				continue
			}
			Schedule(cr, fmt.Sprint(job.id), sch, job)
		}
	}

	log.Println("all job is loaded.")

	expvar.Publish("jobs", expvar.Func(func() interface{} {
		ret := map[string]interface{}{}
		for nm, e := range error_jobs {
			ret[nm] = e.Error()
		}

		for _, ent := range cr.Entries() {
			if export, ok := ent.Job.(Exportable); ok {
				m := export.Stats()
				m["next"] = ent.Next
				m["prev"] = ent.Prev
				ret[ent.Id] = m
			} else {
				ret[ent.Id] = map[string]interface{}{"next": ent.Next, "prev": ent.Prev}
			}
		}

		bs, e := json.MarshalIndent(ret, "", "  ")
		if nil != e {
			return e.Error()
		}
		rm := json.RawMessage(bs)
		return &rm
	}))

	cr.Start()
	defer cr.Stop()

	watcher, e := fsnotify.NewWatcher()
	if e != nil {
		log.Println("[error] new fs watcher failed", e)
		return
	}
	// Process events
	go func() {
		pollInterval := *poll_interval
		if pollInterval < 1*time.Second {
			pollInterval = 1 * time.Second
		}
		for {
			select {
			case ev := <-watcher.Events:
				log.Println("event:", ev)
				if ev.Op&fsnotify.Create == fsnotify.Create {
					nm := strings.ToLower(filepath.Base(ev.Name))
					log.Println("[sys] new job -", nm)
					job, e := loadJobFromFile(ev.Name, arguments)
					if nil != e {
						error_jobs[nm] = e
						log.Println("["+nm+"] schedule failed,", e)
						break
					}
					sch, e := Parse(job.expression)
					if nil != e {
						error_jobs[job.name] = e
						log.Println("["+job.name+"] schedule failed,", e)
						break
					}
					Schedule(cr, job.name, sch, job)
				} else if ev.Op&fsnotify.Remove == fsnotify.Remove {
					nm := strings.ToLower(filepath.Base(ev.Name))
					log.Println("[sys] delete job -", nm)
					cr.Unschedule(nm)
					delete(error_jobs, nm)
				} else if ev.Op&fsnotify.Write == fsnotify.Write || ev.Op&fsnotify.Rename == fsnotify.Rename {
					nm := strings.ToLower(filepath.Base(ev.Name))
					log.Println("[sys] reload job -", nm)
					cr.Unschedule(nm)
					delete(error_jobs, nm)
					job, e := loadJobFromFile(ev.Name, arguments)
					if nil != e {
						error_jobs[nm] = e
						log.Println("["+nm+"] schedule failed,", e)
						break
					}
					sch, e := Parse(job.expression)
					if nil != e {
						error_jobs[job.name] = e
						log.Println("["+job.name+"] schedule failed,", e)
						break
					}
					Schedule(cr, job.name, sch, job)
				}
			case err := <-watcher.Errors:
				log.Println("error:", err)
			case <-time.After(pollInterval):
				if e := reloadJobsFromDB(cr, error_jobs, backend, arguments); nil != e {
					log.Println(e)
				}
			}
		}
	}()

	for _, dir := range job_directories {
		e = watcher.Add(dir)
		if e != nil {
			if dirExists(dir) {
				log.Println("[sys] watch directory '"+dir+"' failed", e)
				return
			}
		}
	}

	log.Println("[schd-jobs] serving at '" + *listenAddress + "'")
	e = http.ListenAndServe(*listenAddress, nil)
	if e != nil {
		log.Fatal(e)
	}
}

func reloadJobsFromDB(cr *cron.Cron, error_jobs map[string]error, backend *dbBackend, arguments map[string]interface{}) error {
	jobs, e := backend.snapshot(nil)
	if nil != e {
		return errors.New("load snapshot from db failed, " + e.Error())
	}
	versions := map[int64]version{}
	for _, v := range jobs {
		versions[v.id] = v
	}

	for _, ent := range cr.Entries() {
		if job, ok := ent.Job.(*JobFromDB); ok {
			if v, ok := versions[job.id]; ok {
				if !v.updated_at.Equal(job.updated_at) {
					reloadJobFromDB(cr, error_jobs, backend, arguments, job.id, job.name)
				}
				delete(versions, job.id)
			} else {
				log.Println("[sys] delete job -", job.name)
				cr.Unschedule(fmt.Sprint(job.id))
				delete(error_jobs, fmt.Sprint(job.id))
			}
		}
	}

	for id, _ := range versions {
		reloadJobFromDB(cr, error_jobs, backend, arguments, id, "")
	}
	return nil
}

func reloadJobFromDB(cr *cron.Cron, error_jobs map[string]error, backend *dbBackend, arguments map[string]interface{}, id int64, name string) {
	message_prefix := "[sys] reload job -"
	if "" == name {
		message_prefix = "[sys] load new job -"
	}

	job, e := backend.find(id)
	if nil != e {
		if "" == name {
			log.Println(message_prefix, "[", id, "]")
		} else {
			log.Println(message_prefix, name)
		}
		return
	}
	e = afterLoad(job, arguments)
	if nil != e {
		log.Println(message_prefix, job.name)
		return
	}

	id_str := fmt.Sprint(id)
	log.Println(message_prefix, job.name)
	cr.Unschedule(id_str)
	delete(error_jobs, id_str)

	sch, e := Parse(job.expression)
	if nil != e {
		msg := errors.New("[" + job.name + "] schedule failed," + e.Error())
		error_jobs[id_str] = msg
		log.Println(msg)
		return
	}
	if nil == sch {
		msg := errors.New("[" + job.name + "] schedule failed, expression '" + job.expression + "' is invalid.")
		error_jobs[id_str] = msg
		log.Println(msg)
		return
	}
	Schedule(cr, id_str, sch, job)
}

func Parse(spec string) (sch cron.Schedule, e error) {
	defer func() {
		if o := recover(); nil != o {
			e = errors.New(fmt.Sprint(o))
		}
	}()

	return cron.Parse(spec)
}

func search_java_home(root string) string {
	java_execute := "java.exe"
	if "windows" != runtime.GOOS {
		java_execute = "java"
	}

	jp := filepath.Join(root, "runtime_env/jdk/bin", java_execute)
	if fileExists(jp) {
		return jp
	}

	jp = filepath.Join(root, "runtime_env/jre/bin", java_execute)
	if fileExists(jp) {
		return jp
	}

	jp = filepath.Join(root, "runtime_env/java/bin", java_execute)
	if fileExists(jp) {
		return jp
	}

	ss, _ := filepath.Glob(filepath.Join(root, "**", java_execute))
	if nil != ss && 0 != len(ss) {
		return ss[0]
	}

	jh := os.Getenv("JAVA_HOME")
	if "" != jh {
		return filepath.Join(jh, "bin", java_execute)
	}

	return java_execute
}

func loadJobsFromDB(backend *dbBackend, arguments map[string]interface{}) ([]*JobFromDB, error) {
	jobs, e := backend.where(nil)
	if nil != e {
		return nil, e
	}
	for _, job := range jobs {
		e = afterLoad(job, arguments)
		if nil != e {
			return nil, e
		}
		log.Println("load '" + job.name + "' is ok.")
	}
	return jobs, nil
}

func afterLoad(job *JobFromDB, arguments map[string]interface{}) error {
	is_java := false
	if "java" == strings.ToLower(job.execute) || "java.exe" == strings.ToLower(job.execute) {
		job.execute = *java_home
		is_java = true
		// } else if "java15" == strings.ToLower(job.execute) || "java15.exe" == strings.ToLower(job.execute) {
		// 	job.execute = *java15_home
		// 	is_java = true
	} else {
		job.execute = executeTemplate(job.execute, arguments)
		execute_tolow := strings.ToLower(job.execute)
		if strings.HasSuffix(execute_tolow, "java") || strings.HasSuffix(execute_tolow, "java.exe") {
			is_java = true
		}
	}

	job.directory = executeTemplate(job.directory, arguments)
	if nil != job.arguments {
		for idx, s := range job.arguments {
			job.arguments[idx] = executeTemplate(s, arguments)
		}

		if is_java {
			for i := 0; i < len(job.arguments); i += 2 {
				if (i + 1) == len(job.arguments) {
					continue
				}

				if "-cp" == strings.TrimSpace(job.arguments[i]) ||
					"-classpath" == strings.TrimSpace(job.arguments[i]) ||
					"--classpath" == strings.TrimSpace(job.arguments[i]) {
					classpath, e := loadJavaClasspath(strings.Split(job.arguments[i+1], ";"))
					if nil != e {
						return errors.New("load classpath of '" + job.name + "' failed, " + e.Error())
					}

					if nil == classpath && 0 == len(classpath) {
						return errors.New("load classpath of '" + job.name + "' failed, it is empty.")
					}

					job.arguments[i] = strings.TrimSpace(job.arguments[i])
					if "windows" == runtime.GOOS {
						job.arguments[i+1] = strings.Join(classpath, ";")
					} else {
						job.arguments[i+1] = strings.Join(classpath, ":")
					}
				}
			}
		}

		if "" != job.name {
			job.logfile = filepath.Join(*log_path, "job_"+job.name+".log")
		} else {
			job.logfile = filepath.Join(*log_path, "job_"+strconv.FormatInt(job.id, 10)+".log")
		}
	}
	if nil != job.environments {
		for idx, s := range job.environments {
			job.environments[idx] = executeTemplate(s, arguments)
		}
	}
	return nil
}

func loadJobsFromDirectory(roots []string, arguments map[string]interface{}) ([]*ShellJob, error) {
	jobs := make([]*ShellJob, 0, 10)
	for _, root := range roots {
		matches, e := filepath.Glob(filepath.Join(root, "*.*"))
		if nil != e {
			if !os.IsNotExist(e) {
				return nil, errors.New("search '" + filepath.Join(root, "*.*") + "' failed, " + e.Error())
			}
		}

		if nil == matches {
			continue
		}

		for _, nm := range matches {
			job, e := loadJobFromFile(nm, arguments)
			if nil != e {
				return nil, errors.New("load '" + nm + "' failed, " + e.Error())
			} else {
				log.Println("load '" + nm + "' is ok.")
			}
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

func ensureLogPath(root string, arguments map[string]interface{}) string {
	logPath := stringWithDefault(arguments, "logPath", "")
	if "" == logPath {
		if runtime.GOOS != "windows" {
			logPath = "/var/log/tpt"
		} else {
			logs := []string{filepath.Clean(abs(filepath.Join(root, "..", "logs"))),
				filepath.Clean(abs(filepath.Join(root, "logs")))}
			for _, s := range logs {
				if dirExists(s) {
					logPath = s
					break
				}
			}
		}
	}

	if !dirExists(logPath) {
		os.Mkdir(logPath, 0666)
	}
	return logPath
}

var funcs = template.FuncMap{
	"joinFilePath": filepath.Join,
	"joinUrlPath": func(base string, paths ...string) string {
		var buf bytes.Buffer
		buf.WriteString(base)

		lastSplash := strings.HasSuffix(base, "/")
		for _, pa := range paths {
			if 0 == len(pa) {
				continue
			}

			if lastSplash {
				if '/' == pa[0] {
					buf.WriteString(pa[1:])
				} else {
					buf.WriteString(pa)
				}
			} else {
				if '/' != pa[0] {
					buf.WriteString("/")
				}
				buf.WriteString(pa)
			}

			lastSplash = strings.HasSuffix(pa, "/")
		}
		return buf.String()
	},
}

func executeTemplate(s string, args map[string]interface{}) string {
	if !strings.Contains(s, "{{") {
		return s
	}
	var buffer bytes.Buffer
	t, e := template.New("default").Funcs(funcs).Parse(s)
	if nil != e {
		panic(errors.New("regenerate string failed, " + e.Error()))
	}
	e = t.Execute(&buffer, args)
	if nil != e {
		panic(errors.New("regenerate string failed, " + e.Error()))
	}
	return buffer.String()
}

func loadJobFromFile(file string, args map[string]interface{}) (*ShellJob, error) {
	t, e := loadTemplateFile(file)
	if nil != e {
		return nil, errors.New("read file failed, " + e.Error())
	}

	args["cd_dir"] = filepath.Dir(file)

	var buffer bytes.Buffer
	e = t.Execute(&buffer, args)
	if nil != e {
		return nil, errors.New("regenerate file failed, " + e.Error())
	}

	var v interface{}
	e = json.Unmarshal(buffer.Bytes(), &v)

	if nil != e {
		log.Println(buffer.String())
		return nil, errors.New("ummarshal file failed, " + e.Error())
	}
	if value, ok := v.(map[string]interface{}); ok {
		return loadJobFromMap(file, []map[string]interface{}{value, args})
	}
	return nil, fmt.Errorf("it is not a map or array - %T", v)
}

func loadJobFromMap(file string, args []map[string]interface{}) (*ShellJob, error) {
	name := strings.ToLower(filepath.Base(file))
	if 0 == len(name) {
		return nil, errors.New("'name' is missing.")
	}
	expression := stringWithArguments(args, "expression", "")
	if "" == expression {
		return nil, errors.New("'expression' is missing.")
	}
	timeout := durationWithArguments(args, "timeout", 10*time.Minute)
	if timeout <= 0*time.Second {
		return nil, errors.New("'killTimeout' must is greate 0s.")
	}
	proc := stringWithArguments(args, "execute", "")
	if 0 == len(proc) {
		return nil, errors.New("'execute' is missing.")
	}
	arguments := stringsWithArguments(args, "arguments", "", nil, false)
	environments := stringsWithArguments(args, "environments", "", nil, false)
	directory := stringWithDefault(args[0], "directory", "")
	if 0 == len(directory) && 1 < len(args) {
		directory = stringWithArguments(args[1:], "root_dir", "")
	}

	switch strings.ToLower(filepath.Base(proc)) {
	case "java", "java.exe":
		var e error
		arguments, e = loadJavaArguments(arguments, args)
		if nil != e {
			return nil, e
		}

		if "java" == proc || "java.exe" == proc {
			proc = *java_home
		}
	}

	logfile := filepath.Join(*log_path, "job_"+name+".log")
	return &ShellJob{name: name,
		mode:         stringWithArguments(args, "mode", ""),
		enabled:      boolWithArguments(args, "enabled", true),
		queue:        stringWithArguments(args, "queue", ""),
		timeout:      timeout,
		expression:   expression,
		execute:      proc,
		directory:    directory,
		environments: environments,
		arguments:    arguments,
		logfile:      logfile}, nil
}
func loadJavaClasspath(cp []string) ([]string, error) {
	if 0 == len(cp) {
		return nil, nil
	}
	var classpath []string
	for _, p := range cp {
		p = strings.TrimSpace(p)
		if 0 == len(p) {
			continue
		}
		files, e := filepath.Glob(p)
		if nil != e {
			return nil, e
		}
		if nil == files {
			continue
		}

		classpath = append(classpath, files...)
	}
	return classpath, nil
}
func loadJavaArguments(arguments []string, args []map[string]interface{}) ([]string, error) {
	var results []string
	classpath, e := loadJavaClasspath(stringsWithArguments(args, "java_classpath", ";", nil, false))
	if nil != e {
		return nil, e
	}

	if nil != classpath && 0 != len(classpath) {
		if "windows" == runtime.GOOS {
			results = append(results, "-cp", strings.Join(classpath, ";"))
		} else {
			results = append(results, "-cp", strings.Join(classpath, ":"))
		}
	}

	debug := stringWithArguments(args, "java_debug", "")
	if 0 != len(debug) {
		suspend := boolWithArguments(args, "java_debug_suspend", false)
		if suspend {
			results = append(results, "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005")
		} else {
			results = append(results, "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005")
		}
	}

	options := stringsWithArguments(args, "java_options", ",", nil, false)
	if nil != options && 0 != len(options) {
		results = append(results, options...)
	}

	class := stringWithArguments(args, "java_class", "")
	if 0 != len(class) {
		results = append(results, strings.TrimSpace(class))
	}

	jar := stringWithArguments(args, "java_jar", "")
	if 0 != len(jar) {
		results = append(results, strings.TrimSpace(jar))
	}

	if nil != arguments && 0 != len(arguments) {
		return append(results, arguments...), nil
	}
	return results, nil
}

func loadConfig(root string) (map[string]interface{}, error) {
	file := ""
	if "" == *config_file || "./<program_name>.conf" == *config_file {
		program := filepath.Base(os.Args[0])
		files := []string{filepath.Clean(abs(filepath.Join(*root_dir, program+".conf"))),
			filepath.Clean(abs(filepath.Join(*root_dir, "etc", program+".conf"))),
			filepath.Clean(abs(filepath.Join(*root_dir, "conf", program+".conf"))),
			filepath.Clean(abs(filepath.Join(*root_dir, "scheduler.conf"))),
			filepath.Clean(abs(filepath.Join(*root_dir, "etc", "scheduler.conf"))),
			filepath.Clean(abs(filepath.Join(*root_dir, "conf", "scheduler.conf")))}

		found := false
		for _, nm := range files {
			if fileExists(nm) {
				found = true
				file = nm
				break
			}
		}

		if !found && *is_print {
			log.Println("config file is not found:")
			for _, nm := range files {
				log.Println("    ", nm)
			}
		}
	} else {
		file = filepath.Clean(abs(*config_file))
		if !fileExists(file) {
			return nil, errors.New("config '" + file + "' is not exists.")
		}
	}

	var arguments map[string]interface{}
	//"autostart_"
	if "" != file {
		var e error
		arguments, e = loadProperties(root, file)
		if nil != e {
			return nil, e
		}
	} else {
		log.Println("[warn] the default config file is not found.")
	}

	if nil == arguments {
		arguments = loadDefault(root, file)
	}

	if _, ok := arguments["java"]; !ok {
		arguments["java"] = *java_home
	}

	arguments["root_dir"] = root
	arguments["config_file"] = file
	arguments["os"] = runtime.GOOS
	arguments["arch"] = runtime.GOARCH
	return arguments, nil
}

func loadDefault(root, file string) map[string]interface{} {
	return map[string]interface{}{"root_dir": root,
		"config_file": file,
		"java":        *java_home,
		"os":          runtime.GOOS,
		"arch":        runtime.GOARCH}
}

func loadProperties(root, file string) (map[string]interface{}, error) {
	t, e := loadTemplateFile(file)
	if nil != e {
		return nil, errors.New("read config failed, " + e.Error())
	}
	args := loadDefault(root, file)

	var buffer bytes.Buffer
	e = t.Execute(&buffer, args)
	if nil != e {
		return nil, errors.New("generate config failed, " + e.Error())
	}

	var arguments map[string]interface{}
	e = json.Unmarshal(buffer.Bytes(), &arguments)
	if nil != e {
		return nil, errors.New("ummarshal config failed, " + e.Error())
	}

	return arguments, nil
}

func loadTemplateFile(file string) (*template.Template, error) {
	bs, e := ioutil.ReadFile(file)
	if nil != e {
		return nil, errors.New("read file failed, " + e.Error())
	}
	return template.New("default").Funcs(funcs).Parse(string(bs))
}
