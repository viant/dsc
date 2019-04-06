package dsc

import (
	"database/sql"
	"fmt"
	"github.com/viant/toolbox"
	"path"
	"strings"
	"time"
)

//TODO refactor for both better dialect and multi version of the same vendor handling

const ansiTableListSQL = "SELECT table_name AS name FROM  information_schema.tables WHERE table_schema = ?"
const ansiSequenceSQL = "SELECT auto_increment AS seq_value FROM information_schema.tables WHERE table_name = '%v' AND table_schema = DATABASE()"
const ansiPrimaryKeySQL = "SELECT column_name AS name FROM information_schema.key_column_usage WHERE table_name = '%v' AND table_schema = '%v' AND constraint_name='PRIMARY'"
const defaultAutoincremetSQL = "SELECT 1 AS autoicrement FROM information_schema.COLUMNS WHERE T TABLE_SCHEMA = '%v'  AND TABLE_NAME = '%v'  AND COLUMN_NAME = '%v'  AND EXTRA like '%auto_increment%'"

const defaultSchemaSQL = "SELECT DATABASE() AS name"
const ansiSchemaListSQL = "SELECT schema_name AS name FROM  information_schema.schemata"

const ansiTableInfo = ` SELECT 
	column_name,
	data_type,
	character_maximum_length AS data_type_length,
	numeric_precision,
	numeric_scale,
	is_nullable
FROM information_schema.columns
WHERE  table_name = '%s' AND table_schema = '%s' 
ORDER BY ordinal_position`

const casandraVersion = "SELECT cql_version AS version from system.local"
const casandraSchemaListV3SQL = "SELECT keyspace_name as name FROM system_schema.keyspaces"
const casandraSchemaListV2SQL = "SELECT keyspace_name AS name FROM system.schema_keyspaces"

const casandraTableListV3SQL = "SELECT table_name AS name  FROM system_schema.tables WHERE keyspace_name = ? ALLOW FILTERING"
const casandraTableListV2SQL = "SELECT columnfamily_name AS name FROM system.schema_columnfamilies WHERE keyspace_name = ? ALLOW FILTERING"

const casandraPrimaryKeyV3SQL = "SELECT column_name AS name FROM system_schema.columns WHERE table_name = '%v' AND keyspace_name = '%v' AND kind='partition_key' ALLOW FILTERING"
const casandraPrimaryKeyV2SQL = "SELECT column_name AS name, type AS pk FROM system.schema_columns WHERE  columnfamily_name = '%v' AND keyspace_name = '%v' ALLOW FILTERING"

const casandraTableInfoV3SQL = ` SELECT column_name AS column_name, type AS data_type, position AS position  FROM system_schema.columns WHERE table_name = '%v' AND keyspace_name = '%v' ALLOW FILTERING`
const casandraTableInfoV2SQL = ` SELECT column_name, validator AS data_type, component_index AS position  FROM system.schema_columns WHERE  columnfamily_name = '%v' AND keyspace_name = '%v' ALLOW FILTERING`

const mysqlDisableForeignCheck = "SET FOREIGN_KEY_CHECKS=0"
const mysqlEnableForeignCheck = "SET FOREIGN_KEY_CHECKS=1"

const sqlLightTableSQL = "SELECT name FROM SQLITE_MASTER WHERE type='table' AND name NOT IN('sqlite_sequence') AND LENGTH(?) > 0"
const sqlLightSequenceSQL = "SELECT COALESCE(MAX(name), 0) + 1   FROM (SELECT seq AS name FROM SQLITE_SEQUENCE WHERE name = '%v')"
const sqlLightSchemaSQL = "PRAGMA database_list"
const sqlLightPkSQL = "pragma table_info(%v);"

const pgCurrentSchemaSQL = "SELECT current_database() AS name"
const pgSchemaListSQL = "SELECT datname AS name FROM pg_catalog.pg_database"

const pgTableListSQL = "SELECT table_name AS name FROM  information_schema.tables WHERE table_catalog = ? AND table_schema = 'public' "

const pgPrimaryKeySQL = `SELECT c.column_name AS name FROM information_schema.key_column_usage u
JOIN information_schema.columns c ON u.column_name = c.column_name AND u.table_name = c.table_name AND u.constraint_catalog = c.table_catalog  
JOIN information_schema.table_constraints tc ON tc.constraint_name = u.constraint_name AND tc.table_name = c.table_name AND tc.constraint_catalog = c.table_catalog  
WHERE u.table_name = c.table_name 
	AND tc.constraint_type = 'PRIMARY KEY'
	AND c.table_name = '%v'    
	AND c.table_catalog = '%v'
ORDER BY u.ordinal_position
`

const pgAutoincrementSQL = `SELECT LIKE(column_default, 'nextval(%v') AS is_autoincrement FROM information_schema.key_column_usage u
JOIN information_schema.columns c ON u.column_name = c.column_name AND u.table_name = c.table_name AND u.constraint_catalog = c.table_catalog  
JOIN information_schema.table_constraints tc ON tc.constraint_name = u.constraint_name AND tc.table_name = c.table_name AND tc.constraint_catalog = c.table_catalog  
WHERE u.table_name =  c.table_name
	AND tc.constraint_type = 'PRIMARY KEY'
	AND c.table_name = '%v'    
	AND c.table_catalog = '%v'
`

const oraTableSQL = `SELECT table_name AS "name" FROM all_tables WHERE owner = ?`
const oraSchemaSQL = `SELECT sys_context( 'userenv', 'current_schema' ) AS "name" FROM dual`
const oraSchemaListSQL = `SELECT USERNAME AS "name"  FROM ALL_USERS`

const oraPrimaryKeySQL = `SELECT c.column_name AS "name"
FROM all_constraints p
JOIN all_cons_columns c ON p.constraint_name = c.constraint_name AND p.owner = c.owner
 WHERE c.table_name = UPPER('%v') 
AND p.owner = UPPER('%v') 
AND p.constraint_type = 'P'
ORDER BY c.position`

const oraTableInfo = `
SELECT 
	COLUMN_NAME AS "column_name",
	DATA_TYPE AS "data_type",
	DATA_LENGTH AS "data_type_length",
	DATA_PRECISION AS "numeric_precision",
	(CASE WHEN NULLABLE = 'Y' THEN 1 END) AS "is_nullable"
FROM ALL_TAB_COLUMNS
WHERE TABLE_NAME = '%s' AND OWNER = '%s'
ORDER BY COLUMN_ID
`

/*

 ` SELECT
	column_name,
	data_type,
	character_maximum_length AS data_type_length,
	numeric_precision,
	numeric_scale,
	is_nullable
FROM information_schema.columns
WHERE  table_name = '%s' AND table_schema = '%s'
ORDER BY ordinal_position`
*/
const msSchemaSQL = "SELECT SCHEMA_NAME() AS name"
const msSequenceSQL = "SELECT current_value AS seq_value FROM sys.sequences WHERE  name = '%v'"
const verticaTableInfo = `SELECT column_name, 
	data_type,
	data_type_length, 
	numeric_precision, 
	numeric_scale,  
	is_nullable 
FROM v_catalog.columns 
WHERE table_name = '%s' AND  table_schema = '%s' 
ORDER BY ordinal_position`

type nameRecord struct {
	Name string `TableColumn:"name"`
}

type sqlDatastoreDialect struct {
	tablesSQL              string
	sequenceSQL            string
	schemaSQL              string
	allSchemaSQL           string
	keySQL                 string
	disableForeignKeyCheck string
	enableForeignKeyCheck  string
	autoIncrementSQL       string
	tableInfoSQL           string
	schemaResultsetIndex   int
	DatastoreDialect
}

//ShowCreateTable returns basic table DDL (this implementation does not check unique and fk constrains)
func (d *sqlDatastoreDialect) ShowCreateTable(manager Manager, table string) (string, error) {
	datastore, err := d.DatastoreDialect.GetCurrentDatastore(manager)
	if err != nil {
		return "", err
	}
	columns, err := d.DatastoreDialect.GetColumns(manager, datastore, table)
	if err != nil {
		return "", fmt.Errorf("unable to get columns for %v.%v, %v", datastore, table, err)
	}
	pkColumns := d.DatastoreDialect.GetKeyName(manager, datastore, table)
	if err != nil {
		return "", fmt.Errorf("unable to get pk key for %v.%v, %v", datastore, table, err)
	}
	var indexPk = map[string]bool{}
	for _, key := range strings.Split(pkColumns, ",") {
		indexPk[key] = true
	}
	var projection = make([]string, 0)
	var keyColumns = make([]string, 0)
	for _, column := range columns {
		var dataType = column.DatabaseTypeName()
		ddlColumn := fmt.Sprintf("%v %v", column.Name(), dataType)
		if nullable, ok := column.Nullable(); ok && !nullable {
			ddlColumn += " NOT NULL "
		}
		if indexPk[column.Name()] {
			ddlColumn += " PRIMARY KEY "
			keyColumns = append(keyColumns, ddlColumn)
			continue
		}
		projection = append(projection, ddlColumn)
	}
	projection = append(keyColumns, projection...)
	return fmt.Sprintf("CREATE TABLE %v(\n\t%v);", table, strings.Join(projection, ",\n\t")), nil
}

func (d sqlDatastoreDialect) Ping(manager Manager) error {
	provider := manager.ConnectionProvider()
	connection, err := provider.Get()
	if err != nil {
		return err
	}
	defer connection.Close()
	dbConnection, err := asSQLDb(connection.Unwrap((*sql.DB)(nil)))
	if err != nil {
		return err
	}
	err = dbConnection.Ping()
	if err == nil {
		return nil
	}
	errMessage := strings.ToLower(err.Error())
	if strings.Contains(errMessage, "access denied") { //connected but issue with password
		return nil
	}
	if strings.Contains(errMessage, "password") { //connected but issue with password
		return nil
	}
	if strings.Contains(errMessage, "user") { //connected but issue with password
		return nil
	}
	return err
}

func (d sqlDatastoreDialect) CanHandleTransaction() bool {
	return true
}

//CanDropDatastore returns true if this dialect can create datastore
func (d sqlDatastoreDialect) CanCreateDatastore(manager Manager) bool {
	return true
}

//CanDropDatastore returns true if this dialect can drop datastore
func (d sqlDatastoreDialect) CanDropDatastore(manager Manager) bool {
	return true
}

func (d sqlDatastoreDialect) Init(manager Manager, connection Connection) error {
	return nil
}

func hasColumns(columns []*sql.ColumnType) bool {
	if len(columns) == 0 {
		return false
	}
	return columns[0].DatabaseTypeName() != ""
}

func (d sqlDatastoreDialect) GetColumns(manager Manager, datastore, tableName string) ([]Column, error) {
	provider := manager.ConnectionProvider()
	connection, err := provider.Get()
	if err != nil {
		return nil, err
	}
	defer connection.Close()

	dbConnection, err := asSQLDb(connection.Unwrap((*sql.DB)(nil)))
	if err != nil {
		return nil, err
	}
	var source = datastore + "." + tableName
	currentDb, err := d.GetCurrentDatastore(manager)
	if err == nil && currentDb == datastore {
		source = tableName
	}

	var query = "SELECT * FROM " + source + " WHERE 1 = 0"
	rows, err := dbConnection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("unable to query: %v, %v", query, err)
	}
	defer rows.Close()
	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var result = make([]Column, 0)

	if !hasColumns(columns) {
		tableInfoSQL := fmt.Sprintf(d.tableInfoSQL, tableName, datastore)
		var tableColumns = []*TableColumn{}
		err := manager.ReadAll(&tableColumns, tableInfoSQL, []interface{}{}, nil)

		if err == nil {
			for _, column := range tableColumns {
				if index := strings.Index(column.DataType, "("); index != -1 {
					column.DataType = string(column.DataType[:index])
				}
				column.DataType = strings.ToUpper(column.DataType)
				result = append(result, column)
			}
			return result, nil
		}
	} else {
		for _, column := range columns {
			result = append(result, column)
		}
	}
	return result, nil
}

func (d oraDialect) GetColumns(manager Manager, datastore, tableName string) ([]Column, error) {
	provider := manager.ConnectionProvider()
	connection, err := provider.Get()
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	dbConnection, err := asSQLDb(connection.Unwrap((*sql.DB)(nil)))
	if err != nil {
		return nil, err
	}
	var source = datastore + "." + tableName
	currentDb, err := d.GetCurrentDatastore(manager)
	if err == nil && currentDb == datastore {
		source = tableName
	}
	var result = make([]Column, 0)
	tableInfoSQL := fmt.Sprintf(oraTableInfo, tableName, datastore)
	var tableColumns = []*TableColumn{}
	err = manager.ReadAll(&tableColumns, tableInfoSQL, []interface{}{}, nil)
	if err == nil {
		for _, column := range tableColumns {
			if index := strings.Index(column.DataType, "("); index != -1 {
				column.DataType = string(column.DataType[:index])
			}

			column.DataType = strings.ToUpper(column.DataType)
			if column.DataTypeLength != nil && strings.Contains(column.DataType, "CHAR") {
				column.DataType += fmt.Sprintf("(%d)", *column.DataTypeLength)
			}
			result = append(result, column)
		}
		return result, nil
	}
	var query = "SELECT * FROM " + source + " WHERE 1 = 0"
	rows, err := dbConnection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("unable to query: %v, %v", query, err)
	}
	defer rows.Close()
	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	for _, column := range columns {
		result = append(result, column)
	}
	return result, nil
}

func (d sqlDatastoreDialect) EachTable(manager Manager, handler func(table string) error) error {
	dbName, err := d.GetCurrentDatastore(manager)
	if err != nil {
		return err
	}
	tables, err := d.GetTables(manager, dbName)
	if err != nil {
		return err
	}
	for _, table := range tables {
		if err := handler(table); err != nil {
			return err
		}
	}
	return err
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
func (d sqlDatastoreDialect) CreateTable(manager Manager, datastore string, table string, specification interface{}) error {
	_, err := manager.Execute(fmt.Sprintf("CREATE TABLE %v(%v)", table, specification))
	return err
}

//GetTables return tables names for passed in datastore managed by manager.
func (d sqlDatastoreDialect) GetTables(manager Manager, datastore string) ([]string, error) {
	var rows = make([]nameRecord, 0)
	err := manager.ReadAll(&rows, d.tablesSQL, []interface{}{datastore}, nil)
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

//GetKeyName returns key/PK columns
func (d sqlDatastoreDialect) GetKeyName(manager Manager, datastore, table string) string {
	if d.keySQL == "" {
		return ""
	}
	SQL := fmt.Sprintf(d.keySQL, table, datastore)
	var records = make([]map[string]interface{}, 0)

	err := manager.ReadAll(&records, SQL, []interface{}{}, nil)
	if err != nil {
		return ""
	}
	var result = make([]string, 0)
	for _, item := range records {
		if pk, ok := item["pk"]; ok {
			if toolbox.AsBoolean(pk) {
				result = append(result, toolbox.AsString(item["name"]))
			}
			continue
		}
		result = append(result, toolbox.AsString(item["name"]))
	}
	return strings.Join(result, ",")
}

//GetDatastores returns name of datastores, takes  manager as parameter
func (d sqlDatastoreDialect) GetDatastores(manager Manager) ([]string, error) {
	var rows = make([][]interface{}, 0)
	err := manager.ReadAll(&rows, d.allSchemaSQL, nil, nil)
	if err != nil {
		if strings.Contains(err.Error(), "unable to open database") {
			return []string{}, nil
		}
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
	if d.schemaSQL == "" {
		return "", nil
	}
	var result = make([]interface{}, 0)
	success, err := manager.ReadSingle(&result, d.schemaSQL, nil, nil)
	if err != nil || !success {
		return "", err
	}
	return normalizeName(toolbox.AsString(result[d.schemaResultsetIndex])), nil

}

func (d sqlDatastoreDialect) IsAutoincrement(manager Manager, datastore, table string) bool {
	if d.autoIncrementSQL == "" {
		return false
	}
	datastore, err := d.GetCurrentDatastore(manager)
	if err != nil {
		return false
	}
	var key = d.GetKeyName(manager, datastore, table)
	var result = make([]interface{}, 0)
	success, err := manager.ReadSingle(&result, fmt.Sprintf(d.autoIncrementSQL, datastore, table, key), nil, nil)
	if err != nil || !success {
		return false
	}
	if len(result) == 1 {
		return toolbox.AsInt(result[0]) == 1
	}
	return false
}

//GetSequence returns sequence value or error for passed in manager and table/sequence
func (d sqlDatastoreDialect) GetSequence(manager Manager, name string) (int64, error) {
	var result = make([]interface{}, 0)
	var sequenceError error
	if d.sequenceSQL != "" {
		var success bool
		success, sequenceError = manager.ReadSingle(&result, fmt.Sprintf(d.sequenceSQL, name), []interface{}{}, nil)
		if success && len(result) == 1 {
			var intResult = toolbox.AsInt(result[0])
			if intResult > 0 {
				return int64(intResult), nil
			}
		}
	}
	datastore, err := d.GetCurrentDatastore(manager)
	if err != nil {
		return 0, err
	}
	var key = d.GetKeyName(manager, datastore, name)
	if key != "" {
		success, err := manager.ReadSingle(&result, fmt.Sprintf("SELECT MAX(%v)  AS seq_value FROM  %v", key, name), []interface{}{}, nil)
		if err != nil || !success {
			return 0, err
		}
		if len(result) == 1 {
			return int64(toolbox.AsInt(result[0]) + 1), nil
		}
	}
	return 0, sequenceError
}

//DisableForeignKeyCheck disables fk check
func (d sqlDatastoreDialect) DisableForeignKeyCheck(manager Manager, connection Connection) error {
	if d.disableForeignKeyCheck == "" {
		return nil
	}
	_, err := manager.ExecuteOnConnection(connection, d.disableForeignKeyCheck, nil)
	return err
}

//EnableForeignKeyCheck disables fk check
func (d sqlDatastoreDialect) EnableForeignKeyCheck(manager Manager, connection Connection) error {
	if d.enableForeignKeyCheck == "" {
		return nil
	}
	_, err := manager.ExecuteOnConnection(connection, d.enableForeignKeyCheck, nil)
	return err
}

func (d sqlDatastoreDialect) NormalizeSQL(SQL string) string {
	return SQL
}

//CanPersistBatch return true if datastore can persist in batch
func (d sqlDatastoreDialect) CanPersistBatch() bool {
	return false
}

//NewSQLDatastoreDialect creates a new default sql dialect
func NewSQLDatastoreDialect(tablesSQL, sequenceSQL, schemaSQL, allSchemaSQL, keySQL, disableForeignKeyCheck, enableForeignKeyCheck, autoIncrementSQL, tableInfoSQL string, schmeaIndex int, dialect DatastoreDialect) *sqlDatastoreDialect {
	return &sqlDatastoreDialect{
		tablesSQL:              tablesSQL,
		sequenceSQL:            sequenceSQL,
		schemaSQL:              schemaSQL,
		allSchemaSQL:           allSchemaSQL,
		keySQL:                 keySQL,
		disableForeignKeyCheck: disableForeignKeyCheck,
		enableForeignKeyCheck:  enableForeignKeyCheck,
		autoIncrementSQL:       autoIncrementSQL,
		tableInfoSQL:           tableInfoSQL,
		schemaResultsetIndex:   schmeaIndex,
		DatastoreDialect:       dialect,
	}
}

type mySQLDialect struct {
	DatastoreDialect
}

func (d mySQLDialect) CanPersistBatch() bool {
	return true
}

func newMySQLDialect() mySQLDialect {
	var result = mySQLDialect{}
	sqlDialect := NewSQLDatastoreDialect(ansiTableListSQL, ansiSequenceSQL, defaultSchemaSQL, ansiSchemaListSQL, ansiPrimaryKeySQL, mysqlDisableForeignCheck, mysqlEnableForeignCheck, defaultAutoincremetSQL, ansiTableInfo, 0, result)
	result.DatastoreDialect = sqlDialect
	sqlDialect.DatastoreDialect = result
	return result
}

type casandraSQLDialect struct {
	*sqlDatastoreDialect
}

func (d casandraSQLDialect) getCQLVersion(manager Manager) (string, error) {
	version := struct{ Version string }{}
	if ok, err := manager.ReadSingle(&version, casandraVersion, nil, nil); ok {
		fragments := strings.Split(version.Version, ".")
		if len(fragments) == 0 {
			return version.Version, err
		}
		return fragments[0], nil
	}
	return "", fmt.Errorf("unable to determine version")
}

func (d casandraSQLDialect) GetColumns(manager Manager, datastore, tableName string) ([]Column, error) {
	var result = make([]Column, 0)
	var err error
	var tableColumns = []*TableColumn{}
	useMapping := false
	if err = manager.ReadAll(&tableColumns, fmt.Sprintf(casandraTableInfoV3SQL, tableName, datastore), []interface{}{}, nil); err != nil {
		err = manager.ReadAll(&tableColumns, fmt.Sprintf(casandraTableInfoV2SQL, tableName, datastore), []interface{}{}, nil)
		useMapping = true
	}
	if err == nil {
		for _, column := range tableColumns {
			if useMapping {
				column.DataType = typeCasandraV2Type(column.DataType)
			}
			column.DataType = strings.ToUpper(column.DataType)
			result = append(result, column)
		}
		return result, nil
	}
	return result, err
}

func typeCasandraV2Type(validator string) string {
	dataType := strings.Replace(validator, "org.apache.cassandra.db.marshal.", "", len(validator))
	dataType = strings.Replace(dataType, "Type", "", len(dataType))
	dataType = strings.Replace(dataType, "UTF8", "text", len(dataType))
	dataType = strings.Replace(dataType, "(", "<", len(dataType))
	dataType = strings.Replace(dataType, ")", ">", len(dataType))
	dataType = strings.ToUpper(dataType)
	dataType = strings.Replace(dataType, "LONG", "BIGINT", len(dataType))
	dataType = strings.Replace(dataType, "INTEGER", "INT", len(dataType))
	dataType = strings.Replace(dataType, "INETADDRESS", "INET", len(dataType))
	dataType = strings.Replace(dataType, "INT32", "INT", len(dataType))
	dataType = strings.Replace(dataType, "BYTES", "BLOB", len(dataType))
	dataType = strings.Replace(dataType, "BYTES", "BLOB", len(dataType))
	return dataType
}

//GetKeyName returns key/PK columns
func (d casandraSQLDialect) GetKeyName(manager Manager, datastore, table string) string {
	if d.keySQL == "" {
		return ""
	}
	var records = make([]map[string]interface{}, 0)
	var err error
	if err = manager.ReadAll(&records, fmt.Sprintf(casandraPrimaryKeyV3SQL, table, datastore), []interface{}{}, nil); err != nil {
		err = manager.ReadAll(&records, fmt.Sprintf(casandraPrimaryKeyV2SQL, table, datastore), []interface{}{}, nil)
	}
	if err != nil {
		return ""
	}
	var result = make([]string, 0)
	for _, item := range records {
		if pk, ok := item["pk"]; ok {
			if toolbox.AsString(pk) == "partition_key" {
				result = append(result, toolbox.AsString(item["name"]))
			}
			continue
		}
		result = append(result, toolbox.AsString(item["name"]))
	}
	return strings.Join(result, ",")
}

func (d casandraSQLDialect) CanPersistBatch() bool {
	return false
}

func (d casandraSQLDialect) NormalizeSQL(SQL string) string {
	SQL = strings.TrimSpace(SQL)
	upperSQL := strings.ToUpper(SQL)
	if strings.HasPrefix(upperSQL, "DELETE") && (!strings.Contains(upperSQL, "WHERE")) {
		if fromIndex := strings.Index(upperSQL, "FROM"); fromIndex != -1 {
			SQL = "TRUNCATE " + string(SQL[fromIndex+5:])
		}
	}
	return SQL
}

func (d casandraSQLDialect) CreateDatastore(manager Manager, datastore string) error {
	_, err := manager.Execute("CREATE KEYSPACE IF NOT EXISTS " + datastore + ` WITH REPLICATION = { 
   'class' : 'SimpleStrategy', 
   'replication_factor' : 1 
  }`)
	if err == nil {
		_, _ = manager.Execute("USE " + datastore)
	}
	return err
}

func (d casandraSQLDialect) CanHandleTransaction() bool {
	return false
}

//GetDatastores returns name of datastores, takes  manager as parameter
func (d casandraSQLDialect) GetDatastores(manager Manager) ([]string, error) {
	var err error
	var rows = make([][]interface{}, 0)
	err = manager.ReadAll(&rows, casandraSchemaListV2SQL, nil, nil)
	if err != nil {
		err = manager.ReadAll(&rows, casandraSchemaListV3SQL, nil, nil)
	}
	if err != nil {
		return nil, err
	}
	var result = make([]string, 0)
	for _, row := range rows {
		result = append(result, normalizeName(toolbox.AsString(row[d.schemaResultsetIndex])))
	}
	return result, nil
}

//DropTable drops a table in datastore managed by manager.
func (d casandraSQLDialect) DropTable(manager Manager, datastore string, table string) error {
	datastore, err := d.GetCurrentDatastore(manager)
	if err != nil {
		return err
	}
	_, err = manager.Execute("DROP TABLE " + datastore + "." + table)
	return err
}

//GetTables return tables names for passed in datastore managed by manager.
func (d casandraSQLDialect) GetTables(manager Manager, datastore string) ([]string, error) {
	var rows = make([]nameRecord, 0)
	err := manager.ReadAll(&rows, casandraTableListV3SQL, []interface{}{datastore}, nil)
	if err != nil {
		err = manager.ReadAll(&rows, casandraTableListV2SQL, []interface{}{datastore}, nil)
	}
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

func (d casandraSQLDialect) DropDatastore(manager Manager, datastore string) error {
	_, err := manager.Execute("DROP KEYSPACE IF EXISTS " + datastore)
	return err
}

func (d casandraSQLDialect) GetCurrentDatastore(manager Manager) (string, error) {
	keySpace := manager.Config().GetString("keyspace", "")
	if keySpace == "" {
		keySpace = manager.Config().GetString("dbname", "")
	}
	if keySpace == "" {
		description := manager.Config().Descriptor
		if index := strings.Index(description, "keyspace="); index != -1 {
			keySpace = string(description[index+len("keyspace="):])
			if index := strings.Index(keySpace, "&"); index != -1 {
				keySpace = string(keySpace[:index])
			}
		}
	}
	return keySpace, nil
}

func newCasandraDialect() *casandraSQLDialect {
	var result = &casandraSQLDialect{}
	sqlDialect := NewSQLDatastoreDialect(casandraTableListV3SQL, ansiSequenceSQL, "", casandraSchemaListV3SQL, casandraPrimaryKeyV3SQL, "", "", "", casandraTableInfoV3SQL, 0, result)
	result.sqlDatastoreDialect = sqlDialect
	sqlDialect.DatastoreDialect = result
	return result
}

type sqlLiteDialect struct {
	DatastoreDialect
}

//CreateDatastore create a new datastore (database/schema), it takes manager and target datastore
func (d sqlLiteDialect) CreateDatastore(manager Manager, datastore string) error {
	return nil
}

//GetSequence returns sequence value or error for passed in manager and table/sequence
func (d sqlLiteDialect) GetSequence(manager Manager, name string) (int64, error) {
	var result = make([]interface{}, 0)
	success, sequenceError := manager.ReadSingle(&result, fmt.Sprintf(sqlLightSequenceSQL, name), []interface{}{}, nil)
	if success && len(result) == 1 {
		var intResult = toolbox.AsInt(result[0])
		if intResult > 0 {
			return int64(intResult), nil
		}
	}
	datastore, err := d.GetCurrentDatastore(manager)
	if err != nil {
		return 0, err
	}
	var key = d.GetKeyName(manager, datastore, name)
	if key != "" {
		success, err := manager.ReadSingle(&result, fmt.Sprintf("SELECT MAX(%v) AS seq_value FROM  %v", key, name), []interface{}{}, nil)
		if err != nil || !success {
			return 0, err
		}
		if len(result) == 1 {
			return int64(toolbox.AsInt(result[0]) + 1), nil
		}
	}
	return 0, sequenceError
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

func (d sqlLiteDialect) GetKeyName(manager Manager, datastore, table string) string {
	var records = make([]map[string]interface{}, 0)
	err := manager.ReadAll(&records, fmt.Sprintf(sqlLightPkSQL, table), []interface{}{}, nil)
	if err != nil {
		return ""
	}
	var result = make([]string, 0)
	for _, item := range records {
		if toolbox.AsString(item["pk"]) == "1" {
			result = append(result, toolbox.AsString(item["name"]))
		}
	}
	return strings.Join(result, ",")
}

func newSQLLiteDialect() *sqlLiteDialect {
	result := &sqlLiteDialect{}
	sqlDialect := NewSQLDatastoreDialect(sqlLightTableSQL, sqlLightSequenceSQL, sqlLightSchemaSQL, sqlLightSchemaSQL, sqlLightPkSQL, "", "", "", ansiTableInfo, 2, result)
	result.DatastoreDialect = sqlDialect
	sqlDialect.DatastoreDialect = result
	return result
}

type pgDialect struct {
	DatastoreDialect
}

func (d pgDialect) CanPersistBatch() bool {
	return true
}

func newPgDialect() *pgDialect {
	result := &pgDialect{}
	sqlDialect := NewSQLDatastoreDialect(pgTableListSQL, "", pgCurrentSchemaSQL, pgSchemaListSQL, pgPrimaryKeySQL, "", "", pgAutoincrementSQL, ansiTableInfo, 0, result)
	result.DatastoreDialect = sqlDialect
	sqlDialect.DatastoreDialect = result
	return result
}

func (d pgDialect) NormalizeSQL(SQL string) string {
	count := 1
	var normalizedSQL = ""
	for _, r := range SQL {
		aChar := string(r)
		if aChar == "?" {
			normalizedSQL += "$" + toolbox.AsString(count)
			count++
		} else {
			normalizedSQL += aChar
		}
	}
	return normalizedSQL
}

func (d pgDialect) IsAutoincrement(manager Manager, datastore, table string) bool {
	datastore, err := d.GetCurrentDatastore(manager)
	if err != nil {
		return false
	}
	var SQL = fmt.Sprintf(pgAutoincrementSQL, "%", table, datastore)
	var result = make([]interface{}, 0)
	success, err := manager.ReadSingle(&result, SQL, nil, nil)
	if err != nil || !success {
		return false
	}
	if len(result) == 1 {
		return toolbox.AsBoolean(result[0])
	}
	return false
}

func (d pgDialect) DisableForeignKeyCheck(manager Manager, connection Connection) error {
	return d.EachTable(manager, func(table string) error {
		_, err := manager.ExecuteOnConnection(connection, fmt.Sprintf("ALTER TABLE %v DISABLE TRIGGER ALL", table), nil)
		return err
	})
}

func (d pgDialect) EnableForeignKeyCheck(manager Manager, connection Connection) error {
	return d.EachTable(manager, func(table string) error {
		_, err := manager.ExecuteOnConnection(connection, fmt.Sprintf("ALTER TABLE %v ENABLE TRIGGER ALL", table), nil)
		return err
	})
}

type oraDialect struct {
	DatastoreDialect
}

func (d oraDialect) CanPersistBatch() bool {
	return true
}

//CreateDatastore create a new datastore (database/schema), it takes manager and target datastore
func (d oraDialect) CreateDatastore(manager Manager, datastore string) error {
	var password, ok = manager.Config().Parameters["password"]
	if !ok {
		return fmt.Errorf("password was empty")
	}
	DCL := fmt.Sprintf("CREATE USER %v IDENTIFIED BY %v", datastore, password)
	if _, err := manager.Execute(DCL); err != nil {
		return err
	}
	DCL = fmt.Sprintf("GRANT CONNECT, RESOURCE, DBA TO %v", datastore)
	if _, err := manager.Execute(DCL); err != nil {
		return err
	}
	return nil
}

//DropTable drops a datastore (database/schema), it takes manager and datastore to be droped
func (d oraDialect) DropDatastore(manager Manager, datastore string) error {
	_, err := manager.Execute(fmt.Sprintf("DROP USER %v CASCADE", datastore))
	return err
}

func (d oraDialect) NormalizeSQL(SQL string) string {
	count := 1
	var normalizedSQL = ""
	for _, r := range SQL {
		aChar := string(r)
		if aChar == "?" {
			normalizedSQL += ":" + toolbox.AsString(count)
			count++
		} else {
			normalizedSQL += aChar
		}
	}
	return normalizedSQL
}

func (d oraDialect) Init(manager Manager, connection Connection) error {
	config := manager.Config()
	if _, has := config.Parameters["session"]; !has {
		return nil
	}
	session := config.GetMap("session")
	if session == nil {
		return nil
	}

	for k, v := range session {
		_, err := manager.ExecuteOnConnection(connection, fmt.Sprintf("ALTER SESSION SET %v = '%v'", k, v), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func newOraDialect() *oraDialect {
	result := &oraDialect{}
	sqlDialect := NewSQLDatastoreDialect(oraTableSQL, "", oraSchemaSQL, oraSchemaListSQL, oraPrimaryKeySQL, "", "", "", ansiTableInfo, 0, result)
	result.DatastoreDialect = sqlDialect
	sqlDialect.DatastoreDialect = result
	return result
}

type odbcDialect struct {
	DatastoreDialect
}

func (d *odbcDialect) Init(manager Manager, connection Connection) error {
	searchPath := manager.Config().Get("SEARCH_PATH")
	if searchPath != "" {

		var SQL = fmt.Sprintf("SET SEARCH_PATH=%v", searchPath)
		if _, err := manager.ExecuteOnConnection(connection, SQL, nil); err != nil {
			return err
		}
	}
	timezone := manager.Config().Get("TIMEZONE")
	if timezone != "" {
		var SQL = fmt.Sprintf("SET TIMEZONE TO '%v'", timezone)
		if _, err := manager.ExecuteOnConnection(connection, SQL, nil); err != nil {
			return err
		}
		//ODBC driver harcoding issue
		if location, err := time.LoadLocation(timezone); err == nil {
			time.Local = location
		}
	}
	return nil
}

func newOdbcDialect() *odbcDialect {
	result := &odbcDialect{}
	sqlDialect := NewSQLDatastoreDialect(ansiTableListSQL, "", "", ansiSchemaListSQL, "", "", "", "", verticaTableInfo, 0, result)
	result.DatastoreDialect = sqlDialect
	sqlDialect.DatastoreDialect = result
	return result
}

type msSQLDialect struct {
	DatastoreDialect
}

func newMsSQLDialect() *msSQLDialect {
	result := &msSQLDialect{}
	sqlDialect := NewSQLDatastoreDialect(ansiTableListSQL, msSequenceSQL, msSchemaSQL, ansiSchemaListSQL, "", "", "", "", ansiTableInfo, 0, result)
	result.DatastoreDialect = sqlDialect
	sqlDialect.DatastoreDialect = result
	return result
}
