package dsc

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"
)

func asSQLDb(wrapped interface{}) (*sql.DB, error) {
	if result, ok := wrapped.(*sql.DB); ok {
		return result, nil
	}
	wrappedType := reflect.ValueOf(wrapped)
	return nil, fmt.Errorf(fmt.Sprintf("failed cast as sql.DB: was %v !", wrappedType.Type()))
}

func asSQLTx(wrapped interface{}) (*sql.Tx, error) {
	if wrapped == nil {
		return nil, nil
	}
	if result, ok := wrapped.(*sql.Tx); ok {
		return result, nil
	}
	wrappedType := reflect.ValueOf(wrapped)
	return nil, fmt.Errorf(fmt.Sprintf("failed cast as sql.Tx: was %v !", wrappedType.Type()))
}

func asScanner(wrapped interface{}) (Scanner, error) {
	if result, ok := reflect.ValueOf(wrapped).Interface().(Scanner); ok {
		return result, nil
	}
	wrappedType := reflect.ValueOf(wrapped)
	return nil, fmt.Errorf(fmt.Sprintf("failed cast as Scannable: was %v %v !", wrappedType.Type(), reflect.ValueOf(wrapped).Elem()))
}

type sqlExecutor interface {
	Exec(sql string, parameters ...interface{}) (sql.Result, error)
}

type sqlManager struct {
	*AbstractManager
}

func (m *sqlManager) initConnectionIfNeeded(connection Connection) error {
	if sqlConnection, ok := connection.(*sqlConnection); ok {
		if sqlConnection.init {
			return nil
		}
		sqlConnection.init = true
		dialect := GetDatastoreDialect(m.config.DriverName)
		return dialect.Init(m, connection)
	}
	return nil
}

func (m *sqlManager) ExecuteOnConnection(connection Connection, sql string, args []interface{}) (sql.Result, error) {
	m.Acquire()
	db, err := asSQLDb(connection.Unwrap(sqlDbPointer))
	if err == nil {
		err = m.initConnectionIfNeeded(connection)
	}
	if err != nil {
		return nil, err
	}

	var executable sqlExecutor = db
	tx, err := asSQLTx(connection.Unwrap(sqlTxtPointer))
	if err != nil {
		return nil, err
	}
	if tx != nil {
		executable = tx
	}
	if args == nil {
		args = make([]interface{}, 0)
	}

	dialect := GetDatastoreDialect(m.config.DriverName)
	sql = dialect.NormalizeSQL(sql)
	result, err := executable.Exec(sql, args...)
	if !dialect.CanHandleTransaction() {
		result = NewSQLResult(1, 0)
	}
	Logf("[%v]:%v %v", m.config.username, sql, args)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("failed to execute %v %v on %v due to:\n%v", sql, args, m.Manager.Config().Parameters, err.Error()))
	}
	return result, err
}

func (m *sqlManager) ReadAllOnWithHandlerOnConnection(connection Connection, query string, args []interface{}, readingHandler func(scanner Scanner) (toContinue bool, err error)) error {
	m.Acquire()
	startTime := time.Now()
	db, err := asSQLDb(connection.Unwrap((*sql.DB)(nil)))
	if err == nil {
		err = m.initConnectionIfNeeded(connection)
	}
	if err != nil {
		return err
	}

	dialect := GetDatastoreDialect(m.config.DriverName)
	query = dialect.NormalizeSQL(query)
	Logf("[%v]:%v", m.config.username, query)

	sqlStatement, sqlError := db.Prepare(query)
	if sqlError != nil {
		return fmt.Errorf("failed to prepare sql: %v with %v due to:%v\n\t", query, args, sqlError.Error())
	}

	Logf("[%v]:prepare time: %v\n", m.config.username, time.Now().Sub(startTime))

	defer sqlStatement.Close()
	rows, queryError := m.executeQuery(sqlStatement, query, args)
	if queryError != nil {
		return fmt.Errorf(fmt.Sprintf("failed to execute sql: %v with %v due to:%v\n\t", query, args, queryError.Error()))
	}
	Logf("[%v]:execute time: %v\n", m.config.username, time.Now().Sub(startTime))

	defer rows.Close()
	for rows.Next() {
		scanner, _ := asScanner(rows)
		toContinue, err := readingHandler(NewScanner(scanner))
		if err != nil {
			return err
		}
		if !toContinue {
			break
		}
	}
	Logf("[%v]:fetched time: %v\n", m.config.username, time.Now().Sub(startTime))
	return rows.Err()
}

func (m *sqlManager) executeQuery(sqlStatement *sql.Stmt, query string, args []interface{}) (rows *sql.Rows, err error) {
	if args == nil {
		args = make([]interface{}, 0)
	}
	rows, err = sqlStatement.Query(args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
