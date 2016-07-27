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
	panic("Failed to lookup datastore dialectabe: " + driver)
}

func init() {
	RegisterDatastoreDialect("mysql", &mySQLDialect{})
	RegisterDatastoreDialect("pg", &pgDialect{})
	RegisterDatastoreDialect("ora", &oraDialect{})
	RegisterDatastoreDialect("mssql", &msSQLDialect{})
	RegisterDatastoreDialect("sqlite3", &sqlLiteDialect{})
	RegisterDatastoreDialect("ndjson", &fileDialect{})
}
