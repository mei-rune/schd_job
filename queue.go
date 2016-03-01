package sched_job

import (
	"sync/atomic"
	"time"

	"sync"
)

type worker struct {
	status int32
	C      chan *ShellJob
}
type queueThread struct {
	wait   sync.WaitGroup
	closed int32
	C      chan *ShellJob

	queues map[string]*worker
}

func (q *queueThread) Close() {
	if !atomic.CompareAndSwapInt32(&q.closed, 0, 1) {
		return
	}
	close(q.C)
	q.wait.Wait()
}

func (q *queueThread) run() {
	defer q.wait.Done()
	timer := time.NewTicker(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			q.clear()
			return
		case job, ok := <-q.C:
			if !ok {
				return
			}
			q.runJob(job)
		}
	}
}

func (q *queueThread) clear() {

}

func (q *queueThread) runSub(name string, w *worker) {
	defer atomic.StoreInt32(&w.status, 1)
	for job := range w.C {
		if 0 != atomic.LoadInt32(&q.closed) {
			go job.RunInQueue(false)
		} else {
			job.RunInQueue(true)
		}
	}
}

func (q *queueThread) runJob(job *ShellJob) {
	if 0 != atomic.LoadInt32(&q.closed) {
		go job.RunInQueue(false)
		return
	}

	sub, ok := q.queues[job.name]
	if !ok {
		sub = &worker{C: make(chan *ShellJob, 200)}
		q.queues[job.name] = sub
		q.wait.Add(1)
		go func() {
			defer q.wait.Done()
			q.runSub(job.name, sub)
		}()
	}

	select {
	case sub.C <- job:
	default:
		go job.RunInQueue(false)
	}
}

func newQueue() *queueThread {
	queue := &queueThread{
		//closed: make(chan struct{}),
		C:      make(chan *ShellJob, 1000),
		queues: make(map[string]*worker),
	}

	queue.wait.Add(1)
	go queue.run()
	return queue
}
