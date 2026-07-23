package schd_job

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	_ "gitee.com/chunanyong/dm"                       // 达梦
	_ "gitee.com/opengauss/openGauss-connector-go-pq" // openGauss
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"
	_ "github.com/ziutek/mymysql/godrv"
)

var (
	OpenGaussUrl  = "host=192.168.1.202 port=8888 user=golang password=123456_go dbname=golang sslmode=disable"
	PostgreSQLUrl = "host=127.0.0.1 user=golang password=123456 dbname=golang sslmode=disable"
	MySQLUrl      = "golang:123456@tcp(localhost:3306)/golang?autocommit=true&parseTime=true&multiStatements=true"
	MsSqlUrl      = "sqlserver://golang:123456@127.0.0.1?database=golang&connection+timeout=30"
	DMSqlUrl      = "dm://" + os.Getenv("dm_username") + ":" + os.Getenv("dm_password") + "@" + os.Getenv("dm_host") + "?noConvertToHex=true"
)

var (
	TestDrv     string
	TestConnURL string
)

func init() {
	flag.StringVar(&TestDrv, "dbDrv", "postgres", "")
	flag.StringVar(&TestConnURL, "dbURL", "", "缺省值会根据 dbDrv 的值自动选择，请见 GetTestConnURL()")
}

func GetTestConnDrv() string {
	return TestDrv
}

func GetTestConnURL() string {
	if TestConnURL == "" {
		switch TestDrv {
		case "opengauss":
			return OpenGaussUrl
		case "postgres", "", "pgx", "pgx/v5":
			return PostgreSQLUrl
		case "mysql":
			return MySQLUrl
		case "sqlserver", "mssql":
			return MsSqlUrl
		case "dm":
			return DMSqlUrl
		}
	}

	return TestConnURL
}

// quoteIdent quotes an identifier for use in SQL statements.
func quoteIdent(drv, name string) string {
	switch DbType(drv) {
	case ORACLE:
		return "\"" + name + "\""
	case MYSQL, MariaDB:
		return "`" + name + "`"
	default:
		return name
	}
}

// paramPlaceholder returns the parameter placeholder for the given driver.
func paramPlaceholder(drv string) string {
	switch DbType(drv) {
	case ORACLE, DM:
		return ":"
	case POSTGRESQL, KINGBASE, OPENGAUSS, GAUSSDB:
		return "$"
	default:
		return "?"
	}
}

// param returns the i-th parameter placeholder.
func param(drv string, i int) string {
	p := paramPlaceholder(drv)
	if p == ":" || p == "$" {
		return fmt.Sprintf("%s%d", p, i)
	}
	return p
}

func dropTableSQL(drv, name string) string {
	switch DbType(drv) {
	case ORACLE:
		return "BEGIN EXECUTE IMMEDIATE 'DROP TABLE " + name + "'; EXCEPTION WHEN OTHERS THEN NULL; END;"
	default:
		return "DROP TABLE IF EXISTS " + name
	}
}

func createTableSQL(drv, name string) string {
	var idDef, enabledType, idPrimary string
	switch DbType(drv) {
	case ORACLE:
		idDef = "NUMBER"
		enabledType = "NUMBER(1)"
		idPrimary = "PRIMARY KEY(id)"
	case MYSQL, MariaDB:
		idDef = "INT AUTO_INCREMENT"
		enabledType = "TINYINT(1)"
		idPrimary = "PRIMARY KEY(id)"
	case MSSQL:
		idDef = "INT IDENTITY(1,1)"
		enabledType = "BIT"
		idPrimary = "PRIMARY KEY(id)"
	case DM:
		idDef = "INT IDENTITY(1,1)"
		enabledType = "BIT"
		idPrimary = "PRIMARY KEY(id)"
	default:
		// PostgreSQL, opengauss, kingbase, gaussdb
		idDef = "SERIAL"
		enabledType = "BOOLEAN"
		idPrimary = "PRIMARY KEY(id)"
	}

	qMode := quoteIdent(drv, "mode")
	return "CREATE TABLE " + name + ` (
  id                  ` + idDef + `,
  name                varchar(250) NOT NULL,
  ` + qMode + `                varchar(250),
  queue               varchar(250),
  enabled             ` + enabledType + `,
  description         varchar(250),
  expression          varchar(50)  NOT NULL,
  execute             varchar(250) NOT NULL,
  directory           varchar(250),
  arguments           varchar(250),
  environments        varchar(250),
  kill_after_interval integer DEFAULT -1,
  created_at          TIMESTAMP,
  updated_at          TIMESTAMP,

  CONSTRAINT ` + name + `_name_uq UNIQUE(name),
  ` + idPrimary + `
)`
}

func backendTest(t *testing.T, cb func(backend *dbBackend)) {
	drv := GetTestConnDrv()

	backend, e := newBackend(drv, GetTestConnURL())
	if nil != e {
		t.Error(e)
		return
	}
	defer backend.Close()

	// Drop table (ignore error if it doesn't exist)
	backend.db.Exec(dropTableSQL(drv, *table_name))

	_, e = backend.db.Exec(createTableSQL(drv, *table_name))
	if nil != e {
		t.Error(e)
		return
	}
	cb(backend)
}

func TestLoad(t *testing.T) {
	backendTest(t, func(backend *dbBackend) {
		drv := GetTestConnDrv()
		var insertSQL string
		var params []interface{}
		if DbType(drv) == ORACLE {
			insertSQL = `INSERT INTO ` + *table_name + `(id, name, expression, execute, created_at, updated_at)
    VALUES (1, 'abc', '0 0 * * * ?', 'abcd', ` + param(drv, 1) + `, ` + param(drv, 2) + `)`
			params = []interface{}{time.Now(), time.Now()}
		} else {
			insertSQL = `INSERT INTO ` + *table_name + `( name, expression, execute, created_at, updated_at)
    VALUES ('abc', '0 0 * * * ?', 'abcd', ` + param(drv, 1) + `, ` + param(drv, 2) + `)`
			params = []interface{}{time.Now(), time.Now()}
		}
		_, e := backend.db.Exec(insertSQL, params...)
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
		drv := GetTestConnDrv()
		var insertSQL string
		var params []interface{}
		if DbType(drv) == ORACLE {
			insertSQL = `INSERT INTO ` + *table_name + `(id, name, expression, execute, arguments, environments, created_at, updated_at)
    VALUES (1, 'abc', '0 0 * * * ?', '{{js .root_dir}}/abcd', ` + param(drv, 1) + `, ` + param(drv, 2) + `, ` + param(drv, 3) + `, ` + param(drv, 4) + `)`
			params = []interface{}{`-a={{.a1}}
-cp
abc`, `e1={{.a2}}`, time.Now(), time.Now()}
		} else {
			insertSQL = `INSERT INTO ` + *table_name + `( name, expression, execute, arguments, environments, created_at, updated_at)
    VALUES ('abc', '0 0 * * * ?', '{{js .root_dir}}/abcd', ` + param(drv, 1) + `, ` + param(drv, 2) + `, ` + param(drv, 3) + `, ` + param(drv, 4) + `)`
			params = []interface{}{`-a={{.a1}}
-cp
abc`, `e1={{.a2}}`, time.Now(), time.Now()}
		}
		_, e := backend.db.Exec(insertSQL, params...)
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

func TestParse(t *testing.T) {
	_, e := Parse("")
	if nil == e {
		t.Error("not error")
	}
	_, e = Parse("master")
	if nil == e {
		t.Error("not error")
	}
}
