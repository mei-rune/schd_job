package sched_job

import (
	"os"
	"syscall"
)

func killProcess(pid int) error {
	const PROCESS_TERMINATE = 0x0001
	const da = syscall.STANDARD_RIGHTS_READ |
		syscall.PROCESS_QUERY_INFORMATION | syscall.SYNCHRONIZE | PROCESS_TERMINATE
	h, e := syscall.OpenProcess(da, false, uint32(pid))
	if e != nil {
		return os.NewSyscallError("OpenProcess", e)
	}
	defer syscall.CloseHandle(h)

	e = syscall.TerminateProcess(h, 1)
	if nil != e {
		return os.NewSyscallError("TerminateProcess", e)
	}
	return nil
}
