package dsc

import (
	"fmt"
	"github.com/viant/toolbox"
	"path"
	"strings"
)

const defaultTableSql = "SELECT table_name AS name FROM  information_schema.tables WHERE table_schema = ?"
const defaultSequenceSql = "SELECT auto_increment FROM information_schema.tables WHERE table_name = '%v' AND table_schema = DATABASE()"
const defaultSchemaSql = "SELECT DATABASE() AS name"
const defaultAllSchemaSql = "SELECT schema_name AS name FROM  information_schema.schemata"

const schemaSql = "SELECT current_schema() AS name"

const sqlLightTableSql = "SELECT name FROM SQLITE_MASTER WHERE type='table' AND name NOT IN('sqlite_sequence') AND LENGTH(?) > 0"
const sqlLightSequenceSql = "SELECT COALESCE(MAX(name), 0) + 1   FROM (SELECT seq AS name FROM SQLITE_SEQUENCE WHERE name = '%v')"
const sqlLightSchemaSql = "PRAGMA database_list"

const pgSequenceSql = "SELECT currval(%v) + 1"

const oraTableSql = "SELECT table_name AS name  FROM all_tables WHERE owner = ?"
const oraSchemaSql = "SELECT sys_context( 'userenv', 'current_schema' ) AS name FROM dual"
const oraSequenceSql = "SELECT %v.nextval AS name from dual"
const oraAllSchemaSql = "SELECT username AS name FROM all_users"

const msSchemaSql = "SELECT SCHEMA_NAME() AS name"
const msSequenceSql = "SELECT current_value FROM sys.sequences WHERE  name = '%v'"

type nameRecord struct {
	Name string `column:"name"`
}

type sqlDatastoreDialect struct {
	tablesSql            string
	sequenceSql          string
	schemaSql            string
	allSchemaSql         string
	schemaResultsetIndex int
}

//CanDropDatastore returns true if this dialect can create datastore
func (d sqlDatastoreDialect) CanCreateDatastore(manager Manager) bool {
	return true
}

//CanDropDatastore returns true if this dialect can drop datastore
func (d sqlDatastoreDialect) CanDropDatastore(manager Manager) bool {
	return true
}

//CreateDatastore create a new datastore (database/schema), it takes manager and target datastore
func (d sqlDatastoreDialect) CreateDatastore(manager Manager, datastore string) error {
	_, err := manager.Execute("CREATE DATABASE " + datastore)
	return err
}

//DropTable drops a datastore (database/schema), it takes manager and datastore to be droped
func (d sqlDatastoreDialect) DropDatastore(manager Manager, datastore string) error {
	_, err := manager.Execute("DROP DATABASE " + datastore)
	return err
}

//DropTable drops a table in datastore managed by manager.
func (d sqlDatastoreDialect) DropTable(manager Manager, datastore string, table string) error {
	_, err := manager.Execute("DROP TABLE " + table)
	return err
}

//CreateTable creates table on in datastore managed by manager.
func (d sqlDatastoreDialect) CreateTable(manager Manager, datastore string, table string, specification string) error {
	_, err := manager.Execute("CREATE TABLE " + table + "(" + specification + ")")
	return err
}

//GetTables return tables names for passed in datastore managed by manager.
func (d sqlDatastoreDialect) GetTables(manager Manager, datastore string) ([]string, error) {
	var rows = make([]nameRecord, 0)
	err := manager.ReadAll(&rows, d.tablesSql, []interface{}{datastore}, nil)
	if err != nil {
		return nil, err
	}
	var result = make([]string, 0)
	for _, row := range rows {
		if len(row.Name) > 0 {
			result = append(result, row.Name)
		}
	}
	return result, nil
}

func normalizeName(name string) string {
	if !strings.Contains(name, "/") && !strings.Contains(name, "\\") {
		return name
	}
	_, file := path.Split(name)
	return file
}

//GetDatastores returns name of datastores, takes  manager as parameter
func (d sqlDatastoreDialect) GetDatastores(manager Manager) ([]string, error) {
	var rows = make([][]interface{}, 0)
	err := manager.ReadAll(&rows, d.allSchemaSql, nil, nil)
	if err != nil {
		return nil, err
	}
	var result = make([]string, 0)
	for _, row := range rows {
		result = append(result, normalizeName(toolbox.AsString(row[d.schemaResultsetIndex])))
	}
	return result, nil
}

//GetCurrentDatastore returns name of current schema
func (d sqlDatastoreDialect) GetCurrentDatastore(manager Manager) (string, error) {
	var result = make([]interface{}, 0)
	success, err := manager.ReadSingle(&result, d.schemaSql, nil, nil)
	if err != nil || !success {
		return "", err
	}
	return normalizeName(toolbox.AsString(result[d.schemaResultsetIndex])), nil

}

//GetSequence returns sequence value or error for passed in manager and table/sequence
func (d sqlDatastoreDialect) GetSequence(manager Manager, name string) (int64, error) {
	var result = make([]int64, 0)
	success, err := manager.ReadSingle(&result, fmt.Sprintf(d.sequenceSql, name), []interface{}{}, nil)
	if err != nil || !success {
		return 0, err
	}
	return result[0], nil
}

//CanPersistBatch return true if datastore can persist in batch
func (d sqlDatastoreDialect) CanPersistBatch() bool {
	return false
}

func NewSqlDatastoreDialect(tablesSql, sequenceSql, schemaSql, allSchemaSql string, schmeaIndex int) DatastoreDialect {
	return &sqlDatastoreDialect{tablesSql, sequenceSql, schemaSql, allSchemaSql, schmeaIndex}
}

type mySQLDialect struct {
	DatastoreDialect
}

func newMySQLDialect() mySQLDialect {
	return mySQLDialect{DatastoreDialect: NewSqlDatastoreDialect(defaultTableSql, defaultSequenceSql, defaultSchemaSql, defaultAllSchemaSql, 0)}
}

type sqlLiteDialect struct {
	DatastoreDialect
}

//CreateDatastore create a new datastore (database/schema), it takes manager and target datastore
func (d sqlLiteDialect) CreateDatastore(manager Manager, datastore string) error {
	return nil
}

func (d sqlLiteDialect) DropDatastore(manager Manager, datastore string) error {
	tables, err := d.GetTables(manager, datastore)
	if err != nil {
		return err
	}
	for _, table := range tables {
		err := d.DropTable(manager, datastore, table)
		if err != nil {
			return err
		}
	}
	return err
}

func newSqlLiteDialect() *sqlLiteDialect {
	return &sqlLiteDialect{DatastoreDialect: NewSqlDatastoreDialect(sqlLightTableSql, sqlLightSequenceSql, sqlLightSchemaSql, sqlLightSchemaSql, 2)}
}

type pgDialect struct {
	DatastoreDialect
}

func newPgDialect() *pgDialect {
	return &pgDialect{DatastoreDialect: NewSqlDatastoreDialect(sqlLightTableSql, pgSequenceSql, schemaSql, defaultAllSchemaSql, 0)}
}

type oraDialect struct {
	DatastoreDialect
}

func newOraDialect() *oraDialect {
	return &oraDialect{DatastoreDialect: NewSqlDatastoreDialect(oraTableSql, oraSequenceSql, oraSchemaSql, oraAllSchemaSql, 0)}
}

type msSQLDialect struct {
	DatastoreDialect
}

func newMsSQLDialect() *msSQLDialect {
	return &msSQLDialect{DatastoreDialect: NewSqlDatastoreDialect(defaultTableSql, msSequenceSql, msSchemaSql, defaultAllSchemaSql, 0)}
}
