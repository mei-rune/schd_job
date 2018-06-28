package schd_job

import "os"

func killByPid(pid int) error {
	pr, e := os.FindProcess(pid)
	if nil != e {
		return e
	}
	defer pr.Release()
	return pr.Kill()
}
