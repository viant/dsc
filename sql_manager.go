package dsc

import (
	"database/sql"
	"fmt"
	"reflect"
)

func asSQLDb(wrapped interface{}) (*sql.DB, error) {
	if result, ok := wrapped.(*sql.DB); ok {
		return result, nil
	}
	wrappedType := reflect.ValueOf(wrapped)
	return nil, fmt.Errorf(fmt.Sprintf("Failed cast as sql.DB: was %v !", wrappedType.Type()))
}

func asSQLTx(wrapped interface{}) (*sql.Tx, error) {
	if wrapped == nil {
		return nil, nil
	}
	if result, ok := wrapped.(*sql.Tx); ok {
		return result, nil
	}
	wrappedType := reflect.ValueOf(wrapped)
	return nil, fmt.Errorf(fmt.Sprintf("Failed cast as sql.Tx: was %v !", wrappedType.Type()))
}

func asScanner(wrapped interface{}) (Scanner, error) {
	if result, ok := reflect.ValueOf(wrapped).Interface().(Scanner); ok {
		return result, nil
	}
	wrappedType := reflect.ValueOf(wrapped)
	return nil, fmt.Errorf(fmt.Sprintf("Failed cast as Scannable: was %v %v !", wrappedType.Type(), reflect.ValueOf(wrapped).Elem()))
}

type sqlExecutor interface {
	Exec(sql string, parameters ...interface{}) (sql.Result, error)
}

type sqlManager struct {
	*AbstractManager
}

func (m *sqlManager) ExecuteOnConnection(connection Connection, sql string, args []interface{}) (sql.Result, error) {
	db, err := asSQLDb(connection.Unwrap(sqlDbPointer))
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

	result, err := executable.Exec(sql, args...)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("Failed to execute %v %v on %v due to:\n%v", sql, args, m.Manager.Config().Parameters, err.Error()))
	}
	return result, err
}

func (m *sqlManager) ReadAllOnWithHandlerOnConnection(connection Connection, query string, args []interface{}, readingHandler func(scanner Scanner) (toContinue bool, err error)) error {
	db, err := asSQLDb(connection.Unwrap((*sql.DB)(nil)))
	if err != nil {
		return err
	}

	sqlStatement, sqlError := db.Prepare(query)
	if sqlError != nil {
		return fmt.Errorf("Failed to prepare sql: %v with %v due to:%v\n\t", query, args, sqlError.Error())
	}
	defer sqlStatement.Close()
	rows, queryError := m.executeQuery(sqlStatement, query, args)
	if queryError != nil {
		return fmt.Errorf(fmt.Sprintf("Failed to execute sql: %v with %v due to:%v\n\t", query, args, queryError.Error()))
	}
	defer rows.Close()
	for rows.Next() {
		scanner, _ := asScanner(rows)
		toContinue, err := readingHandler(scanner)
		if err != nil {
			return err
		}
		if !toContinue {
			break
		}
	}
	return nil
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
