package sched_job

import (
	"reflect"
	"testing"
	"time"
)

func backendTest(t *testing.T, cb func(backend *dbBackend)) {
	// e := Main()
	// if nil != e {
	// 	t.Error(e)
	// 	return
	// }

	backend, e := newBackend(*db_drv, *db_url)
	if nil != e {
		t.Error(e)
		return
	}
	defer backend.Close()

	_, e = backend.db.Exec(`
	DROP TABLE IF EXISTS ` + *table_name + `;

	CREATE TABLE IF NOT EXISTS ` + *table_name + ` (
	  id                  serial   PRIMARY KEY,
	  name                varchar(250) NOT NULL,
	  mode                varchar(250),
	  description         varchar(250),
	  expression          varchar(50)  NOT NULL,
	  execute             varchar(250) NOT NULL,
	  directory           varchar(250),
	  arguments           varchar(250),
	  environments        varchar(250),
	  kill_after_interval integer DEFAULT -1,
	  created_at          timestamp,
	  updated_at          timestamp,

	  CONSTRAINT ` + *table_name + `_name_uq unique(name)
	);`)
	if nil != e {
		t.Error(e)
		return
	}
	cb(backend)
}

func TestLoad(t *testing.T) {
	backendTest(t, func(backend *dbBackend) {
		_, e := backend.db.Exec(`INSERT INTO `+*table_name+`( name, expression, execute, created_at, updated_at)
    VALUES ('abc', '0 0 * * * ?', 'abcd', $1, $2);`, time.Now(), time.Now())
		if nil != e {
			t.Error(e)
			return
		}

		jobs, e := backend.where(nil)
		if nil != e {
			t.Error(e)
			return
		}

		if 1 != len(jobs) {
			t.Error("len of jobs is error, ", len(jobs))
			return
		}

		if "abc" != jobs[0].name {
			t.Error("name is error ", jobs[0].name)
		}
		if "0 0 * * * ?" != jobs[0].expression {
			t.Error("expression is error ", jobs[0].expression)
		}
		if "abcd" != jobs[0].execute {
			t.Error("execute is error ", jobs[0].execute)
		}
	})
}

func TestLoad2(t *testing.T) {
	backendTest(t, func(backend *dbBackend) {
		_, e := backend.db.Exec(`INSERT INTO `+*table_name+`( name, expression, execute, arguments, environments, created_at, updated_at)
    VALUES ('abc', '0 0 * * * ?', '{{js .root_dir}}/abcd', $1, $2, $3, $4);`, `-a={{.a1}}
-cp
abc`, `e1={{.a2}}`, time.Now(), time.Now())
		if nil != e {
			t.Error(e)
			return
		}

		jobs, e := loadJobsFromDB(backend, map[string]interface{}{"root_dir": "c:/test", "a1": "b1", "a2": "b2"})
		if nil != e {
			t.Error(e)
			return
		}

		if 1 != len(jobs) {
			t.Error("len of jobs is error, ", len(jobs))
			return
		}

		if "abc" != jobs[0].name {
			t.Error("name is error ", jobs[0].name)
		}
		if "0 0 * * * ?" != jobs[0].expression {
			t.Error("expression is error ", jobs[0].expression)
		}
		if "c:/test/abcd" != jobs[0].execute {
			t.Error("execute is error ", jobs[0].execute)
		}

		if !reflect.DeepEqual([]string{"-a=b1", "-cp", "abc"}, jobs[0].arguments) {
			t.Error(jobs[0].arguments)
		}
		if !reflect.DeepEqual([]string{"e1=b2"}, jobs[0].environments) {
			t.Error(jobs[0].environments)
		}
	})
}
