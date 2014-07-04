package sched_job

import (
	"os"
	"os/exec"
	"testing"
)

func TestKillProcess(t *testing.T) {
	t.Skip("this is bug for the golang library")
	pr := exec.Command("ping", "127.0.0.1", "-t")
	e := pr.Start()
	if nil != e {
		t.Error(e)
		return
	}
	kp, _ := os.FindProcess(pr.Process.Pid)
	e = kp.Kill()

	if nil != e {
		t.Error(e)
		pr.Process.Kill()
		return
	} else {
		t.Error("it is fix?")
	}
}

func TestKillProcess2(t *testing.T) {
	pr := exec.Command("ping", "127.0.0.1", "-t")
	e := pr.Start()
	if nil != e {
		t.Error(e)
		return
	}
	kp, _ := os.FindProcess(pr.Process.Pid)
	e = killProcess(kp.Pid)

	if nil != e {
		t.Error(e)
		pr.Process.Kill()
		return
	}
}
