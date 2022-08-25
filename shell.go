package schd_job

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kardianos/osext"
	"github.com/runner-mei/cron"
)

var (
	RunMode          string
	ExecutableFolder string
	queues           = map[string]*queueLock{}
	queue_lock       sync.Mutex

	Commands = map[string]string{}

	// RunHook func(job Job) bool
)

// type Job interface {
// 	Exec(context.Context)

// 	ToMap() map[string]interface{}
// }

func init() {
	RunMode = os.Getenv("DAEMON_RUN_MODE")
	ExecutableFolder, _ = osext.ExecutableFolder()
	fillCommands(ExecutableFolder)
}

const maxNum = 5
const maxBytes = 5 * 1024 * 1024

type queueLock struct {
	lock   sync.Mutex
	lastAt time.Time
}

func (ql *queueLock) Lock() {
	ql.lock.Lock()
}

func (ql *queueLock) Unlock() {
	ql.lock.Unlock()
}

func GetQueueLock(name string) *queueLock {
	queue_lock.Lock()
	defer queue_lock.Unlock()

	q := queues[name]
	if nil == q {
		q = &queueLock{}
		queues[name] = q
	}

	if len(queues) > 1000 {
		panic("queue is too many.")
	}
	return q
}

type ShellJob struct {
	id           int64
	opts         JobOption
	enabled      bool
	execute      string
	directory    string
	environments []string
	arguments    []string
	logfile      string
	status       int32

	attributes map[string]interface{}

	instance interface {
		GetVersioner
		Job
	}
}

type JobFromDB struct {
	ShellJob
}

var _ GetVersioner = &JobFromDB{}

func (w *JobFromDB) GetVersion() (int64, time.Time, bool) {
	return w.opts.ID, w.opts.UpdatedAt, true
}

func (w *JobFromDB) Run(ctx context.Context) {
	w.run(ctx)
}

func (w *JobFromDB) ToJob() cron.Job {
	w.ShellJob.instance = w
	return &w.ShellJob
}

func (self *ShellJob) isMode(mode string) bool {
	if "" == mode || "all" == mode {
		return true
	}
	if "" == self.opts.Mode || "default" == self.opts.Mode {
		return true
	}
	if self.opts.Mode == mode {
		return true
	}
	return false
}

func (self *ShellJob) ToMap() map[string]interface{} {
	return map[string]interface{}{"type": "exec2",
		"command":    self.execute,
		"arguments":  self.arguments,
		"attributes": self.attributes,
		"environments": append(self.environments,
			"schd_job_id="+strconv.FormatInt(int64(self.id), 10),
			"schd_job_name="+self.opts.Name)}
}

func (self *ShellJob) Run() {
	self.run(context.Background())
}

func (self *ShellJob) run(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(&self.status, 0, 1) {
		log.Println("[" + self.opts.Name + "] running!")
		return
	}
	defer atomic.StoreInt32(&self.status, 0)

	if !self.enabled {
		log.Println("[" + self.opts.Name + "] is disabled.")
		return
	}

	if !self.isMode(RunMode) {
		log.Println("[" + self.opts.Name + "] should run on '" + self.opts.Mode + "', but current mode is '" + RunMode + "'.")
		return
	}

	if "" != self.opts.Queue {
		q := GetQueueLock(self.opts.Queue)
		log.Println("["+self.opts.Name+"] try entry queue", self.opts.Queue, ".")
		q.Lock()
		defer func() {
			q.Unlock()
			log.Println("["+self.opts.Name+"] exit queue", self.opts.Queue, ".")
		}()
		q.lastAt = time.Now()
		log.Println("["+self.opts.Name+"] already entry queue", self.opts.Queue, ".")
	}

	// if nil != RunHook && RunHook(self) {
	// 	log.Println("[" + self.opts.Name + "] run it with hook.")
	// 	return
	// }

	e := self.rotateLogfile()
	if nil != e {
		log.Println("["+self.opts.Name+"] rotate log file failed,", e)
	}

	self.Exec(ctx)
}

func (self *ShellJob) rotateLogfile() error {
	st, err := os.Stat(self.logfile)
	if nil != err { // file exists
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if st.Size() < maxBytes {
		return nil
	}

	fname2 := self.logfile + fmt.Sprintf(".%04d", maxNum)
	_, err = os.Stat(fname2)
	if nil == err {
		err = os.Remove(fname2)
		if err != nil {
			return err
		}
	}

	fname1 := fname2
	for num := maxNum - 1; num > 0; num-- {
		fname2 = fname1
		fname1 = self.logfile + fmt.Sprintf(".%04d", num)

		_, err = os.Stat(fname1)
		if nil != err {
			continue
		}
		err = os.Rename(fname1, fname2)
		if err != nil {
			return err
		}
	}

	err = os.Rename(self.logfile, fname1)
	if err != nil {
		return err
	}
	return nil
}

func LookPath(executableFolder, pa string, alias ...string) (string, bool) {
	if filepath.IsAbs(pa) {
		return pa, true
	}

	names := []string{pa, pa + ".sh"}
	if runtime.GOOS == "windows" {
		names = []string{pa, pa + ".bat", pa + ".com", pa + ".exe"}
	}
	for _, aliasName := range alias {
		if runtime.GOOS == "windows" {
			names = append(names, aliasName, aliasName+".bat", aliasName+".com", aliasName, aliasName+".exe")
		} else {
			names = append(names, aliasName, aliasName+".sh")
		}
	}

	for _, nm := range names {
		files := []string{nm,
			filepath.Join("bin", nm),
			filepath.Join("tools", nm),
			filepath.Join("runtime_env", nm),
			filepath.Join("..", nm),
			filepath.Join("..", "bin", nm),
			filepath.Join("..", "tools", nm),
			filepath.Join("..", "runtime_env", nm),
			filepath.Join(executableFolder, nm),
			filepath.Join(executableFolder, "bin", nm),
			filepath.Join(executableFolder, "tools", nm),
			filepath.Join(executableFolder, "runtime_env", nm),
			filepath.Join(executableFolder, "..", nm),
			filepath.Join(executableFolder, "..", "bin", nm),
			filepath.Join(executableFolder, "..", "tools", nm),
			filepath.Join(executableFolder, "..", "runtime_env", nm)}
		for _, file := range files {
			file = abs(file)
			if st, e := os.Stat(file); nil == e && nil != st && !st.IsDir() {
				return file, true
			}
		}
	}

	for _, nm := range names {
		_, err := exec.LookPath(nm)
		if nil == err {
			return nm, true
		}
	}
	return "", false
}

func fillCommands(executableFolder string) {
	for _, nm := range []string{"snmpget", "snmpgetnext", "snmpdf", "snmpbulkget",
		"snmpbulkwalk", "snmpdelta", "snmpnetstat", "snmpset", "snmpstatus",
		"snmptable", "snmptest", "snmptools", "snmptranslate", "snmptrap", "snmpusm",
		"snmpvacm", "snmpwalk", "wshell"} {
		if pa, ok := LookPath(executableFolder, nm); ok {
			Commands[nm] = pa
		} else if pa, ok := LookPath(executableFolder, "netsnmp/"+nm); ok {
			Commands[nm] = pa
		}
	}

	if pa, ok := LookPath(executableFolder, "tpt"); ok {
		Commands["tpt"] = pa
	}
	if pa, ok := LookPath(executableFolder, "nmap/nping"); ok {
		Commands["nping"] = pa
	}
	if pa, ok := LookPath(executableFolder, "nmap/nmap"); ok {
		Commands["nmap"] = pa
	}
	if pa, ok := LookPath(executableFolder, "putty/plink", "ssh"); ok {
		Commands["plink"] = pa
		Commands["ssh"] = pa
	}
	if pa, ok := LookPath(executableFolder, "dig/dig", "dig"); ok {
		Commands["dig"] = pa
	}
}



func (self *ShellJob) Exec(ctx context.Context) {
	out, e := OpenFile(self.logfile, self.opts.Name)
	if nil != e {
			log.Println("["+self.opts.Name+"] open log file("+self.logfile+") failed,", e)
			return
	}
	defer CloseFile(out)

	execPath := self.execute
	if s := Commands[self.execute]; s != "" {
		s = execPath
	}

	executePath, found := LookPath(ExecutableFolder, execPath)
	if !found {
		executePath = self.execute
	}

	if ctx == nil {
		ctx = context.Background()
	}
	if self.opts.Timeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, self.opts.Timeout)
		defer cancel()
	}

	cmd := exec.CommandContext(ctx, executePath, self.arguments...)
	cmd.Stderr = out
	cmd.Stdout = out

	var environments []string
	if nil != self.environments && 0 != len(self.environments) {
		osEnv := os.Environ()
		environments = make([]string, 0, len(self.arguments)+len(osEnv)+3)
		environments = append(environments, osEnv...)
		environments = append(environments, self.environments...)
	} else {
		osEnv := os.Environ()
		environments = make([]string, 0, len(osEnv)+3)
		environments = append(environments, osEnv...)
		environments = append(environments, self.environments...)
	}

	environments = append(environments, "schd_job_id="+fmt.Sprint(self.opts.ID))
	environments = append(environments, "schd_job_name="+self.opts.Name)
	cmd.Env = environments

	io.WriteString(out, cmd.Path)

	for idx, s := range cmd.Args {
		if 0 == idx {
			continue
		}
		io.WriteString(out, "\r\n \t\t")
		io.WriteString(out, s)
	}
	io.WriteString(out, "\r\n===============  out  ===============\r\n")
	if e = cmd.Start(); nil != e {
		io.WriteString(out, "start failed, "+e.Error()+"\r\n")
		return
	}

	if self.opts.Timeout <= 0 {
		if e = cmd.Wait(); nil != e {
			io.WriteString(out, "run failed, "+e.Error()+"\r\n")
		} else if nil != cmd.ProcessState {
			io.WriteString(out, "run ok, exit with "+cmd.ProcessState.String()+".\r\n")
		}
		return
	}

	c := make(chan error, 10)
	go func() {
		c <- cmd.Wait()
	}()

	select {
	case e := <-c:
		if nil != e {
			io.WriteString(out, "run failed, "+e.Error()+"\r\n")
		} else if nil != cmd.ProcessState {
			io.WriteString(out, "run ok, exit with "+cmd.ProcessState.String()+".\r\n")
		}
	case <-time.After(self.opts.Timeout):
		killByPid(cmd.Process.Pid)
		io.WriteString(out, "run timeout, kill it.\r\n")
	}
}
