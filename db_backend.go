package schd_job

import (
	"bufio"
	"bytes"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strconv"
	"strings"
	"time"

	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

const (
	AUTO       = 0
	POSTGRESQL = 1
	MYSQL      = 2
	MSSQL      = 3
	ORACLE     = 4
	DB2        = 5
	SYBASE     = 6
	DM         = 7
	KINGBASE   = 8
	OPENGAUSS = 9
)

var (
	is_test_for_lock = false
	test_ch_for_lock = make(chan int)
)

func DbType(drv string) int {
	switch drv {
	case "kingbase", "kingbase8":
		return KINGBASE
	case "postgres":
		return POSTGRESQL
	case "opengauss":
		return OPENGAUSS
	case "mysql", "mymysql":
		return MYSQL
	case "odbc_with_mssql", "mssql", "sqlserver":
		return MSSQL
	case "oci8", "odbc_with_oracle", "oracle", "ora":
		return ORACLE
	case "dm":
		return DM
	default:
		if strings.Contains(drv, "oracle") {
			return ORACLE
		}
		if strings.Contains(drv, "sqlserver") {
			return MSSQL
		}
		if strings.Contains(drv, "db2") {
			return DB2
		}
		if strings.Contains(drv, "sybase") {
			return SYBASE
		}
		return AUTO
	}
}

func I18n(dbType int, drv string, e error) error {
	return i18n(dbType, drv, e)
}

func i18n(dbType int, drv string, e error) error {
	if ORACLE == dbType && "oci8" == drv {
		decoder := simplifiedchinese.GB18030.NewDecoder()
		msg, _, err := transform.String(decoder, e.Error())
		if nil == err {
			return errors.New(msg)
		}
	}
	return e
	// if ORACLE == dbType && "oci8" == drv {
	// 	return errors.New(decoder.ConvertString(e.Error()))
	// }
	// return e
}

func i18nString(dbType int, drv string, e error) string {
	if ORACLE == dbType && "oci8" == drv {
		decoder := simplifiedchinese.GB18030.NewDecoder()
		msg, _, err := transform.String(decoder, e.Error())
		if nil == err {
			return msg
		}
	}
	return e.Error()
	// if ORACLE == dbType && "oci8" == drv {
	// 	return decoder.ConvertString(e.Error())
	// }
	// return e.Error()
}

func IsNumericParams(dbType int) bool {
	switch dbType {
	case ORACLE, DM, POSTGRESQL, KINGBASE, OPENGAUSS:
		return true
	default:
		return false
	}
}

// NullTime represents an time that may be null.
// NullTime implements the Scanner interface so
// it can be used as a scan destination, similar to NullTime.
type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Int64 is not NULL
}

// Scan implements the Scanner interface.
func (n *NullTime) Scan(value interface{}) error {
	if value == nil {
		n.Time, n.Valid = time.Time{}, false
		return nil
	}
	// fmt.Println("wwwwwwwwwwwww", value)
	n.Time, n.Valid = value.(time.Time)
	if !n.Valid {
		if s, ok := value.(string); ok {
			var e error
			for _, layout := range []string{"2006-01-02 15:04:05.000000000", "2006-01-02 15:04:05.000000", "2006-01-02 15:04:05.000", "2006-01-02 15:04:05", "2006-01-02"} {
				if n.Time, e = time.ParseInLocation(layout, s, time.UTC); nil == e {
					n.Valid = true
					break
				}
			}
		}
	}
	return nil
}

// Value implements the driver Valuer interface.
func (n NullTime) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Time, nil
}

const SelectSqlString = "SELECT id, name, mode, enabled, queue, expression, execute, directory, arguments, environments, kill_after_interval, created_at, updated_at "
const WhereQueryString = " WHERE (enabled IS NULL OR enabled = true)"

func SplitLines(bs string) []string {
	res := make([]string, 0, 10)
	line_scanner := bufio.NewScanner(bytes.NewReader([]byte(bs)))
	line_scanner.Split(bufio.ScanLines)

	for line_scanner.Scan() {
		res = append(res, line_scanner.Text())
	}

	if nil != line_scanner.Err() {
		panic(line_scanner.Err())
	}
	return res
}

func NewDbJobLoader(drv string, db *sql.DB, tableName string) (SimpleLoader, error) {
	return &dbJobLoader{
		drv:       drv,
		db:        db,
		dbType:    DbType(drv),
		tableName: tableName,
	}, nil
}

type dbJobLoader struct {
	drv       string
	dbType    int
	db        *sql.DB
	tableName string
}

func (loader *dbJobLoader) Snapshots() (map[int64]time.Time, error) {
	rows, e := loader.db.Query("select id, updated_at from " + loader.tableName + WhereQueryString)
	if nil != e {
		if sql.ErrNoRows == e {
			return nil, nil
		}
		return nil, i18n(loader.dbType, loader.drv, e)
	}
	defer rows.Close()

	var results = map[int64]time.Time{}
	for rows.Next() {
		var job int64
		var updatedAt NullTime

		e = rows.Scan(&job, &updatedAt)
		if nil != e {
			return nil, i18n(loader.dbType, loader.drv, e)
		}
		if updatedAt.Valid {
			results[job] = updatedAt.Time
		} else {
			results[job] = time.Time{}
		}
	}
	e = rows.Err()
	if nil != e {
		return nil, i18n(loader.dbType, loader.drv, e)
	}
	return results, nil
}

func (loader *dbJobLoader) Load(id int64, arguments map[string]interface{}) (JobOption, TimeSchedule, Job, error) {
	row := loader.db.QueryRow(SelectSqlString + " FROM " + loader.tableName + " WHERE id = " + strconv.FormatInt(id, 10))
	opts, job, err := scanJob(loader.dbType, loader.drv, row, arguments)
	if err != nil {
		return JobOption{}, nil, nil, errWrap(err, "获取规则失败")
	}

	sch, err := Parse(opts.Expression)
	if err != nil {
		return JobOption{}, nil, nil, errWrap(err, "解析规则的 ScheduleExpression("+opts.Expression+") 出错")
	}

	return opts, sch, job, nil
}

type rowScanner interface {
	Scan(dest ...interface{}) error
}

func scanJob(dbType int, drv string, scan rowScanner, opts map[string]interface{}) (JobOption, Job, error) {
	var id int64
	var name string
	var mode sql.NullString
	var enabled sql.NullBool
	var queue sql.NullString
	var expression string
	var execute string
	var directory sql.NullString
	var arguments sql.NullString
	var environments sql.NullString
	var killAfterInterval sql.NullInt64
	var createdAt NullTime
	var updatedAt NullTime
	e := scan.Scan(
		&id,
		&name,
		&mode,
		&enabled,
		&queue,
		&expression,
		&execute,
		&directory,
		&arguments,
		&environments,
		&killAfterInterval,
		&createdAt,
		&updatedAt)
	if nil != e {
		return JobOption{}, nil, i18n(dbType, drv, e)
	}

	jobopts := JobOption{
		ID:   id,
		Name: name,
		// Timeout    time.Duration
		Expression: expression,
	}

	if mode.Valid {
		jobopts.Mode = mode.String
	}
	if queue.Valid {
		jobopts.Queue = queue.String
	}

	if killAfterInterval.Valid {
		jobopts.Timeout = time.Duration(killAfterInterval.Int64) * time.Second
	}

	if createdAt.Valid {
		jobopts.CreatedAt = createdAt.Time
	}

	if updatedAt.Valid {
		jobopts.UpdatedAt = updatedAt.Time
	}

	if jobopts.Name == "" {
		jobopts.Name = "auto_gen_" + strconv.FormatInt(id, 10)
	}

	if Onload != nil {
		job, err := Onload(id,
			jobopts.Name,
			execute,
			directory,
			arguments,
			environments,
			createdAt.Time,
			updatedAt.Time)
		if job != nil && err == nil {
			return jobopts, job, err
		}
	}

	job := new(JobFromDB)
	job.opts = jobopts
	job.execute = execute

	if directory.Valid {
		job.directory = directory.String
	}

	if enabled.Valid {
		job.enabled = enabled.Bool
	} else {
		job.enabled = true
	}

	if arguments.Valid && "" != arguments.String {
		job.arguments = SplitLines(arguments.String) // arguments.String
	}

	if environments.Valid && "" != environments.String {
		job.environments = SplitLines(environments.String) // environments.String
	}

	err := afterLoad(job, opts)
	if err != nil {
		return jobopts, nil, errWrap(err, "获取规则失败")
	}

	return jobopts, job, nil
}

func errWrap(err error, msg string) error {
	return errors.New(msg + ": " + err.Error())
}

var Onload func(id int64,
	name string,
	execute string,
	directory sql.NullString,
	arguments sql.NullString,
	environments sql.NullString,
	createdAt time.Time,
	updatedAt time.Time) (Job, error)
