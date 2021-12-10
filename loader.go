package schd_job

import (
	"context"
	"errors"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/runner-mei/cron"
)

type JobOption struct {
	Loader     string
	ID         int64
	UUID       string
	Name       string
	Mode       string
	Queue      string
	Timeout    time.Duration
	Expression string

	UpdatedAt time.Time
	CreatedAt time.Time
}

type TimeSchedule = cron.Schedule

type Job interface {
	Run(context.Context)
}

type GetVersioner interface {
	GetVersion() (int64, time.Time, bool)
}

type Loader interface {
	Info() interface{}
	Load(cr *cron.Cron, arguments map[string]interface{}) error
}

var loaders = map[string]Loader{}

type SimpleLoader interface {
	Snapshots() (map[int64]time.Time, error)
	Load(id int64, arguments map[string]interface{}) (JobOption, TimeSchedule, Job, error)
}

func RegisterLoader(name string, loader SimpleLoader) {
	if _, ok := loaders[name]; ok {
		panic(errors.New("loader '" + name + "' is already exists"))
	}
	loaders[name] = &DefaultLoader{
		Name:      name,
		Snapshots: loader.Snapshots,
		Read:      loader.Load,
	}
}

type DefaultLoader struct {
	Name      string
	Snapshots func() (map[int64]time.Time, error)
	Read      func(id int64, arguments map[string]interface{}) (JobOption, TimeSchedule, Job, error)
	fails     map[int64]string
	lastErr   string
}

func (loader *DefaultLoader) Info() interface{} {
	result := map[string]interface{}{}
	if loader.fails != nil {
		for id, err := range loader.fails {
			result[GenerateJobID(loader.Name, id)] = err
		}
	}

	if loader.lastErr != "" {
		result["error"] = loader.lastErr
	}

	return result
}

func GenerateJobID(loader string, id int64) string {
	return loader + "-$$-" + strconv.FormatInt(id, 10)
}

func HasLoaderName(uuid, loader string) bool {
	return strings.HasPrefix(uuid, loader+"-$$-")
}

func ReadJobID(id string) (string, string) {
	ss := strings.SplitN(id, "-$$-", 2)
	if len(ss) == 2 {
		return ss[0], ss[1]
	}
	return id, ""
}

func (loader *DefaultLoader) Load(cr *cron.Cron, arguments map[string]interface{}) error {
	loader.lastErr = ""

	versions, e := loader.Snapshots()
	if nil != e {
		loader.lastErr = "load snapshot from db failed, " + e.Error()
		return errors.New(loader.lastErr)
	}

	if versions == nil {
		versions = map[int64]time.Time{}
	}

	for _, ent := range cr.Entries() {
		if !HasLoaderName(ent.Id, loader.Name) {
			continue
		}
		id, oldVersion, ok := GetVersion(ent.Job)
		if !ok {
			continue
		}

		if newVersion, ok := versions[id]; ok {
			if newVersion.Equal(oldVersion) {
				delete(versions, id)
				continue
			}

			opts, sch, job, err := loader.Read(id, arguments)
			if err != nil {
				log.Println("["+loader.Name+"] reload '"+strconv.FormatInt(id, 10)+"' fail,", err)

				if loader.fails == nil {
					loader.fails = map[int64]string{}
				}
				loader.fails[id] = err.Error()
				continue
			}
			if opts.ID == 0 {
				opts.ID = id
			}
			opts.Loader = loader.Name
			opts.UUID = GenerateJobID(loader.Name, id)
			cr.Unschedule(ent.Id)
			Schedule(cr, opts.UUID, sch, jobWarp(job, opts))
			delete(versions, id)
			if loader.fails != nil {
				delete(loader.fails, id)
			}

			log.Println("[" + loader.Name + "] reload '" + strconv.FormatInt(id, 10) + "' ok")
		} else {
			log.Println("["+loader.Name+"] delete job -", id)
			cr.Unschedule(ent.Id)

			if loader.fails != nil {
				delete(loader.fails, id)
			}
		}
	}

	for id := range versions {
		opts, sch, job, err := loader.Read(id, arguments)
		if err != nil {
			log.Println("["+loader.Name+"] load '"+strconv.FormatInt(id, 10)+"' fail,", err)
			if loader.fails == nil {
				loader.fails = map[int64]string{}
			}
			loader.fails[id] = err.Error()
			continue
		}
		if opts.ID == 0 {
			opts.ID = id
		}
		opts.Loader = loader.Name
		opts.UUID = GenerateJobID(loader.Name, id)
		Schedule(cr, opts.UUID, sch, jobWarp(job, opts))

		log.Println("["+loader.Name+"] load '"+opts.UUID+"' successful and next time is", sch.Next(time.Now()))
	}

	return nil
}

func GetVersion(job cron.Job) (int64, time.Time, bool) {
	switch w := job.(type) {
	case *ShellJob:
		if w.instance != nil {
			return w.instance.GetVersion()
		}
		return 0, time.Time{}, false
	case GetVersioner:
		return w.GetVersion()
	default:
		return 0, time.Time{}, false
	}
}

func jobWarp(job Job, opts JobOption) cron.Job {
	// _, ok := job.(*warpJob)
	// if ok {
	// 	return job
	// }
	dbJob, ok := job.(*JobFromDB)
	if ok {
		return dbJob.ToJob()
	}
	// _, ok = job.(*ShellJob)
	// if ok {
	// 	return job
	// }

	return &warpJob{
		opts: opts,
		job:  job,
	}
}

var _ GetVersioner = &warpJob{}

type warpJob struct {
	opts   JobOption
	job    Job
	status int32
}

func (w *warpJob) GetVersion() (int64, time.Time, bool) {
	return w.opts.ID, w.opts.UpdatedAt, true
}

func (self *warpJob) isMode(mode string) bool {
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

func (self *warpJob) Run() {
	if !atomic.CompareAndSwapInt32(&self.status, 0, 1) {
		log.Println("[" + self.opts.UUID + ":" + self.opts.Name + "] running!")
		return
	}

	defer func() {
		atomic.StoreInt32(&self.status, 0)
		if r := recover(); r != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			log.Println("["+self.opts.UUID+":"+self.opts.Name+"]: panic running job: %v\n%s", r, buf)
		}
	}()

	if !self.isMode(RunMode) {
		log.Println("[" + self.opts.UUID + ":" + self.opts.Name + "] should run on '" + self.opts.Mode + "', but current mode is '" + RunMode + "'.")
		return
	}

	if "" != self.opts.Queue {
		q := GetQueueLock(self.opts.Queue)
		log.Println("["+self.opts.UUID+":"+self.opts.Name+"] try entry queue", self.opts.Queue, ".")
		q.Lock()
		defer func() {
			q.Unlock()
			log.Println("["+self.opts.UUID+":"+self.opts.Name+"] exit queue", self.opts.Queue, ".")
		}()
		q.lastAt = time.Now()
		log.Println("["+self.opts.UUID+":"+self.opts.Name+"] already entry queue", self.opts.Queue, ".")
	}

	ctx := context.Background()
	if self.opts.Timeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, self.opts.Timeout)
		defer cancel()
	}
	self.job.Run(ctx)
}
