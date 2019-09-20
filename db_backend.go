package schd_job

import (
	"bufio"
	"bytes"
	"database/sql"
	"database/sql/driver"

	"errors"
	"flag"
	"fmt"

	//_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"

	//_ "github.com/runner-mei/go-oci8"
	//_ "github.com/ziutek/mymysql/godrv"
	"strconv"
	"strings"
	"time"
)

const (
	AUTO       = 0
	POSTGRESQL = 1
	MYSQL      = 2
	MSSQL      = 3
	ORACLE     = 4
	DB2        = 5
)

var (
	db_url     = flag.String("db.url", "host=127.0.0.1 dbname=schd_test user=schdtest password=123456 sslmode=disable", "the db url")
	db_drv     = flag.String("db.drv", "postgres", "the db driver")
	db_type    = flag.Int("db.type", AUTO, "the db type, 0 is auto")
	table_name = flag.String("db.table", "sched_jobs", "the table name for jobs")

	is_test_for_lock = false
	test_ch_for_lock = make(chan int)
)

func DbType(drv string) int {
	switch drv {
	case "postgres":
		return POSTGRESQL
	case "mysql", "mymysql":
		return MYSQL
	case "odbc_with_mssql":
		return MSSQL
	case "oci8", "odbc_with_oracle":
		return ORACLE
	default:
		return AUTO
	}
}

func SetTable(table_name string) {
	flag.Set("db.table", table_name)
}

func SetDbUrl(drv, url string) {
	flag.Set("db.url", url)
	flag.Set("db.drv", drv)
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

func IsNumericParams(drv string) bool {
	switch drv {
	case "postgres", "oracle", "odbc_with_oracle", "oci8":
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

// A job object that is persisted to the database.
// Contains the work object as a YAML field.
type dbBackend struct {
	drv               string
	dbType            int
	db                *sql.DB
	select_sql_string string
}

func newBackend(drvName, url string) (*dbBackend, error) {
	drv := drvName
	if strings.HasPrefix(drvName, "odbc_with_") {
		drv = "odbc"
	}

	db, e := sql.Open(drv, url)
	if nil != e {
		return nil, e
	}
	return &dbBackend{drv: drv, db: db, dbType: DbType(drvName),
		select_sql_string: "SELECT id, name, mode, enabled, queue, expression, execute, directory, arguments, environments, kill_after_interval, created_at, updated_at FROM " + *table_name + " "}, nil
}

func (self *dbBackend) Close() error {
	self.db.Close()
	return nil
}

func buildSQL(dbType int, params map[string]interface{}) (string, []interface{}, error) {
	if nil == params || 0 == len(params) {
		return "", []interface{}{}, nil
	}

	buffer := bytes.NewBuffer(make([]byte, 0, 900))
	arguments := make([]interface{}, 0, len(params))
	is_first := true
	for k, v := range params {
		if '@' != k[0] {
			continue
		}
		if is_first {
			is_first = false
			buffer.WriteString(" WHERE ")
		} else if 0 != len(arguments) {
			buffer.WriteString(" AND ")
		}

		buffer.WriteString(k[1:])
		if nil == v {
			buffer.WriteString(" IS NULL")
			continue
		}

		if "[notnull]" == v {
			buffer.WriteString(" IS NOT NULL")
			continue
		}

		switch dbType {
		case ORACLE:
			buffer.WriteString(" = :")
			buffer.WriteString(strconv.FormatInt(int64(len(params)+1), 10))
		case POSTGRESQL:
			buffer.WriteString(" = $")
			buffer.WriteString(strconv.FormatInt(int64(len(params)+1), 10))
		default:
			buffer.WriteString(" = ? ")
		}

		arguments = append(arguments, v)
	}

	if groupBy, ok := params["group_by"]; ok {
		if nil == groupBy {
			return "", nil, errors.New("groupBy is empty.")
		}

		s := fmt.Sprint(groupBy)
		if 0 == len(s) {
			return "", nil, errors.New("groupBy is empty.")
		}

		buffer.WriteString(" GROUP BY ")
		buffer.WriteString(s)
	}

	if having_v, ok := params["having"]; ok {
		if nil == having_v {
			return "", nil, errors.New("having is empty.")
		}

		having := fmt.Sprint(having_v)
		if 0 == len(having) {
			return "", nil, errors.New("having is empty.")
		}

		buffer.WriteString(" HAVING ")
		buffer.WriteString(having)
	}

	if order_v, ok := params["order_by"]; ok {
		if nil == order_v {
			return "", nil, errors.New("order is empty.")
		}

		order := fmt.Sprint(order_v)
		if 0 == len(order) {
			return "", nil, errors.New("order is empty.")
		}

		buffer.WriteString(" ORDER BY ")
		buffer.WriteString(order)
	}

	if limit_v, ok := params["limit"]; ok {
		if nil == limit_v {
			return "", nil, errors.New("limit is not a number, actual value is nil")
		}
		limit := fmt.Sprint(limit_v)
		i, e := strconv.ParseInt(limit, 10, 64)
		if nil != e {
			return "", nil, fmt.Errorf("limit is not a number, actual value is '" + limit + "'")
		}
		if i <= 0 {
			return "", nil, fmt.Errorf("limit must is geater zero, actual value is '" + limit + "'")
		}

		if offset_v, ok := params["offset"]; ok {
			if nil == offset_v {
				return "", nil, errors.New("offset is not a number, actual value is nil")
			}
			offset := fmt.Sprint(offset_v)
			i, e = strconv.ParseInt(offset, 10, 64)
			if nil != e {
				return "", nil, fmt.Errorf("offset is not a number, actual value is '" + offset + "'")
			}

			if i < 0 {
				return "", nil, fmt.Errorf("offset must is geater(or equals) zero, actual value is '" + offset + "'")
			}

			buffer.WriteString(" LIMIT ")
			buffer.WriteString(offset)
			buffer.WriteString(" , ")
			buffer.WriteString(limit)
		} else {
			buffer.WriteString(" LIMIT ")
			buffer.WriteString(limit)
		}
	}

	return buffer.String(), arguments, nil
}

func (self *dbBackend) count(params map[string]interface{}) (int64, error) {
	query, arguments, e := buildSQL(self.dbType, params)
	if nil != e {
		return 0, e
	}

	count := int64(0)
	e = self.db.QueryRow("SELECT count(*) FROM "+*table_name+query, arguments...).Scan(&count)
	if nil != e {
		if sql.ErrNoRows == e {
			return 0, nil
		}
		return 0, i18n(self.dbType, self.drv, e)
	}
	return count, nil
}

type rowScanner interface {
	Scan(dest ...interface{}) error
}

func (self *dbBackend) scanJob(scan rowScanner) (job *JobFromDB, e error) {
	job = new(JobFromDB)
	var mode sql.NullString
	var enabled sql.NullBool
	var queue sql.NullString
	var directory sql.NullString
	var arguments sql.NullString
	var environments sql.NullString
	var kill_after_interval sql.NullInt64
	var created_at NullTime
	var updated_at NullTime
	e = scan.Scan(
		&job.id,
		&job.name,
		&mode,
		&enabled,
		&queue,
		&job.expression,
		&job.execute,
		&directory,
		&arguments,
		&environments,
		&kill_after_interval,
		&created_at,
		&updated_at)
	if nil != e {
		return nil, i18n(self.dbType, self.drv, e)
	}

	if directory.Valid {
		job.directory = directory.String
	}

	if mode.Valid {
		job.mode = mode.String
	}

	if enabled.Valid {
		job.enabled = enabled.Bool
	} else {
		job.enabled = true
	}

	if queue.Valid {
		job.queue = queue.String
	}

	if arguments.Valid && "" != arguments.String {
		job.arguments = SplitLines(arguments.String) // arguments.String
	}

	if environments.Valid && "" != environments.String {
		job.environments = SplitLines(environments.String) // environments.String
	}

	if kill_after_interval.Valid {
		job.timeout = time.Duration(kill_after_interval.Int64) * time.Second
	}

	if created_at.Valid {
		job.created_at = created_at.Time
	}

	if updated_at.Valid {
		job.updated_at = updated_at.Time
	}

	if "" == job.name {
		job.name = "auto_gen_" + strconv.FormatInt(job.id, 10)
	}

	return job, nil
}

func (self *dbBackend) where(params map[string]interface{}) ([]*JobFromDB, error) {
	var rows *sql.Rows
	var e error

	if nil != params {
		query, arguments, e := buildSQL(self.dbType, params)
		if nil != e {
			return nil, i18n(self.dbType, self.drv, e)
		}

		rows, e = self.db.Query(self.select_sql_string+query, arguments...)
	} else {
		rows, e = self.db.Query(self.select_sql_string)
	}

	if nil != e {
		if sql.ErrNoRows == e {
			return nil, nil
		}
		return nil, i18n(self.dbType, self.drv, e)
	}
	defer rows.Close()

	var results []*JobFromDB
	for rows.Next() {
		job, e := self.scanJob(rows)
		if nil != e {
			return nil, e
		}

		results = append(results, job)
	}

	e = rows.Err()
	if nil != e {
		return nil, i18n(self.dbType, self.drv, e)
	}
	return results, nil
}

func (self *dbBackend) find(id int64) (*JobFromDB, error) {
	var row *sql.Row

	switch self.dbType {
	case ORACLE:
		row = self.db.QueryRow(self.select_sql_string+"where id = :1", id)
	case POSTGRESQL:
		row = self.db.QueryRow(self.select_sql_string+"where id = $1", id)
	default:
		row = self.db.QueryRow(self.select_sql_string+"where id = ?", id)
	}

	return self.scanJob(row)
}

type version struct {
	id         int64
	updated_at time.Time
}

func (self *dbBackend) snapshot(params map[string]interface{}) ([]version, error) {
	var rows *sql.Rows
	var e error

	if nil != params {
		query, arguments, e := buildSQL(self.dbType, params)
		if nil != e {
			return nil, i18n(self.dbType, self.drv, e)
		}
		rows, e = self.db.Query("select id, updated_at from "+*table_name+" "+query, arguments...)
	} else {
		rows, e = self.db.Query("select id, updated_at from " + *table_name)
	}

	if nil != e {
		if sql.ErrNoRows == e {
			return nil, nil
		}
		return nil, i18n(self.dbType, self.drv, e)
	}
	defer rows.Close()

	var results []version
	for rows.Next() {

		var job version
		var updated_at NullTime

		e = rows.Scan(
			&job.id,
			&updated_at)
		if nil != e {
			return nil, i18n(self.dbType, self.drv, e)
		}

		if updated_at.Valid {
			job.updated_at = updated_at.Time
		}

		results = append(results, job)
	}

	e = rows.Err()
	if nil != e {
		return nil, i18n(self.dbType, self.drv, e)
	}
	return results, nil
}

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
