package sched_job

import (
	"os"
	"runtime"
)

func killByPid(pid int) error {
	if "windows" == runtime.GOOS {
		return killProcess(pid)
	} else {
		pr, e := os.FindProcess(pid)
		if nil != e {
			return e
		}
		defer pr.Release()
		return pr.Kill()
	}
}
