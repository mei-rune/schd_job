package sched_job

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync/atomic"
	"time"
)

var (
	RunMode string
)

func init() {
	RunMode = os.Getenv("DAEMON_RUN_MODE")
}

const maxNum = 5
const maxBytes = 5 * 1024 * 1024

type Job interface {
	Run()
}
type ShellJob struct {
	id           int64
	name         string
	mode         string
	enabled      bool
	execute      string
	directory    string
	environments []string
	arguments    []string
	logfile      string
	timeout      time.Duration
	expression   string
	status       int32
}

type JobFromDB struct {
	ShellJob
	updated_at time.Time
	created_at time.Time
}

func (self *ShellJob) isMode(mode string) bool {
	if "" == mode || "all" == mode {
		return true
	}
	if "" == self.mode || "default" == self.mode {
		return true
	}
	if self.mode == mode {
		return true
	}
	return false
}

func (self *ShellJob) Run() {
	if !atomic.CompareAndSwapInt32(&self.status, 0, 1) {
		log.Println("[" + self.name + "] running!")
		return
	}

	if !self.enabled {
		log.Println("[" + self.name + "] is disabled.")
		return
	}

	if !self.isMode(RunMode) {
		log.Println("[" + self.name + "] should run on '" + self.mode + "', but current mode is '" + RunMode + "'.")
		return
	}

	go func() {
		defer atomic.StoreInt32(&self.status, 0)
		e := self.rotate_file()
		if nil != e {
			log.Println("["+self.name+"] rotate log file failed,", e)
		}
		self.do_run()
	}()
}

func (self *ShellJob) rotate_file() error {
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

func (self *ShellJob) do_run() {
	out, e := os.OpenFile(self.logfile, os.O_APPEND|os.O_CREATE, 0)
	if nil != e {
		log.Println("["+self.name+"] open log file("+self.logfile+") failed,", e)
		return
	}
	defer out.Close()
	io.WriteString(out, "=============== begin ===============\r\n")
	defer io.WriteString(out, "===============  end  ===============\r\n")

	cmd := exec.Command(self.execute, self.arguments...)
	cmd.Stderr = out
	cmd.Stdout = out

	var environments []string
	if nil != self.environments && 0 != len(self.environments) {
		os_env := os.Environ()
		environments = make([]string, 0, len(self.arguments)+len(os_env)+3)
		environments = append(environments, os_env...)
		environments = append(environments, self.environments...)
	} else {
		os_env := os.Environ()
		environments = make([]string, 0, len(os_env)+3)
		environments = append(environments, os_env...)
		environments = append(environments, self.environments...)
	}

	environments = append(environments, "schd_job_id="+fmt.Sprint(self.id))
	environments = append(environments, "schd_job_name="+self.name)
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

	if self.timeout <= 0 {
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
		out.Seek(0, os.SEEK_END)
		if nil != e {
			io.WriteString(out, "run failed, "+e.Error()+"\r\n")
		} else if nil != cmd.ProcessState {
			io.WriteString(out, "run ok, exit with "+cmd.ProcessState.String()+".\r\n")
		}
	case <-time.After(self.timeout):
		killByPid(cmd.Process.Pid)
		out.Seek(0, os.SEEK_END)
		io.WriteString(out, "run timeout, kill it.\r\n")
		log.Println("[" + self.name + "] run timeout, kill it.")
	}
}
