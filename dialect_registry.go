package dsc

var dialect = NewDefaultDialect()
var datastoreDialectableRegistry = make(map[string]DatastoreDialect)

//RegisterDatastoreDialect register DatastoreDialect for a driver.
func RegisterDatastoreDialect(driver string, dialectable DatastoreDialect) {
	datastoreDialectableRegistry[driver] = dialectable
}

//GetDatastoreDialect returns DatastoreDialect for passed in driver.
func GetDatastoreDialect(driver string) DatastoreDialect {
	if result, ok := datastoreDialectableRegistry[driver]; ok {
		return result
	}
	if isSQLDatabase(driver) {
		RegisterDatastoreDialect(driver, newAnsiSQLDialect())
		return datastoreDialectableRegistry[driver]
	}
	panic("failed to lookup datastore dialect: " + driver)
}

func init() {
	RegisterDatastoreDialect("mysql", newMySQLDialect())
	RegisterDatastoreDialect("pg", newPgDialect())
	RegisterDatastoreDialect("postgres", newPgDialect())
	RegisterDatastoreDialect("ora", newOraDialect())
	RegisterDatastoreDialect("oci8", newOraDialect())
	RegisterDatastoreDialect("sqlserver", newMsSQLDialect())
	RegisterDatastoreDialect("sqlite3", newSQLLiteDialect())
	RegisterDatastoreDialect("cql", newCasandraDialect())
	RegisterDatastoreDialect("vertica", newVerticaDialect())
	RegisterDatastoreDialect("odbc", newOdbcDialect())
	RegisterDatastoreDialect("ndjson", &fileDialect{})
	RegisterDatastoreDialect("tsv", &fileDialect{})
	RegisterDatastoreDialect("csv", &fileDialect{})

}
