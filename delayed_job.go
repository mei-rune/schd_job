package sched_job

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

func db_time_now(db_type int) time.Time {
	switch db_type {
	case MSSQL:
		return time.Now() //.UTC()
	case POSTGRESQL:
		return time.Now()
	}
	return time.Now().UTC()
}

func createDelayedQueue(dbType int, db *sql.DB, job *ShellJob) (e error) {
	now := db_time_now(dbType)

	var priority int
	var attempts int
	var queue string
	var handler string
	var handler_id string
	var run_at time.Time

	priority = 0
	attempts = job.attempts
	queue = job.queue
	handler_id = "schd_" + strconv.FormatInt(int64(job.id), 10)
	run_at = now
	handler_bytes, err := json.MarshalIndent(map[string]interface{}{"type": "exec2",
		"command":   job.execute,
		"arguments": job.arguments,
		"environments": append(job.environments,
			"schd_job_id="+strconv.FormatInt(int64(job.id), 10),
			"schd_job_name="+job.name)}, "", "  ")
	if nil != err {
		return err
	}
	handler = string(handler_bytes)

	//1         2         3      4        5           NULL        6       NULL       NULL       NULL       7           8
	//priority, attempts, queue, handler, handler_id, last_error, run_at, locked_at, locked_by, failed_at, created_at, updated_at
	switch dbType {
	case ORACLE:
		// _, e = tx.Exec("INSERT INTO "+*table_name+"(priority, attempts, queue, handler, handler_id, last_error, run_at, locked_at, locked_by, failed_at, created_at, updated_at) VALUES (:1, :2, :3, :4, :5, NULL, :6, NULL, NULL, NULL, :7, :8)",
		//  job.priority, job.attempts, job.queue, job.handler, job.handler_id, job.run_at, now, now)
		// fmt.Println("INSERT INTO "+*table_name+"(priority, attempts, queue, handler, handler_id, last_error, run_at, locked_at, locked_by, failed_at, created_at, updated_at) VALUES (:1, :2, :3, :4, :5, NULL, :6, NULL, NULL, NULL, :7, :8)",
		//  job.priority, job.attempts, job.queue, job.handler, job.handler_id, job.run_at, now, now)
		now_str := now.Format("2006-01-02 15:04:05")
		_, e = db.Exec(fmt.Sprintf("INSERT INTO "+delayed_job_table_name+"(priority, attempts, queue, handler, handler_id, run_at, created_at, updated_at) VALUES (%d, %d, '%s', :1, '%s', TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'), TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'), TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'))",
			priority, attempts, queue, handler_id, run_at.Format("2006-01-02 15:04:05"), now_str, now_str), handler)
		//fmt.Println(fmt.Sprintf("INSERT INTO "+*table_name+"(priority, attempts, queue, handler, handler_id, run_at, created_at, updated_at) VALUES (%d, %d, '%s', :1, '%s', TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'), TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'), TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'))",
		//  job.priority, job.attempts, job.queue, job.handler_id, job.run_at.Format("2006-01-02 15:04:05"), now_str, now_str), job.handler)
	case POSTGRESQL:
		_, e = db.Exec("INSERT INTO "+delayed_job_table_name+"(priority, attempts, queue, handler, handler_id, last_error, run_at, locked_at, locked_by, failed_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, NULL, $6, NULL, NULL, NULL, now(), now())",
			priority, attempts, queue, handler, handler_id, run_at)
		//fmt.Println("INSERT INTO "+*table_name+"(priority, attempts, queue, handler, handler_id, last_error, run_at, locked_at, locked_by, failed_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, NULL, $6, NULL, NULL, NULL, $7, $8)",
		//  job.priority, job.attempts, job.queue, job.handler, job.handler_id, job.run_at, now, now)
	default:
		_, e = db.Exec("INSERT INTO "+delayed_job_table_name+"(priority, attempts, queue, handler, handler_id, last_error, run_at, locked_at, locked_by, failed_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, NULL, ?, NULL, NULL, NULL, ?, ?)",
			priority, attempts, queue, handler, handler_id, run_at, now, now)
		//fmt.Println("INSERT INTO "+*table_name+"(priority, attempts, queue, handler, handler_id, last_error, run_at, locked_at, locked_by, failed_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, NULL, ?, NULL, NULL, NULL, ?, ?)",
		//  job.priority, job.attempts, job.queue, job.handler, job.handler_id, job.run_at, now, now)
	}
	if nil != e {
		return i18n(dbType, "", e)
	}
	return nil
}
