package dsc

var dialect = NewDefaultDialect()
var datastoreDialectableRegistry = make(map[string]DatastoreDialect)

//RegisterDatastoreDialectable register DatastoreDialect for a driver.
func RegisterDatastoreDialectable(driver string, dialectable DatastoreDialect) {
	datastoreDialectableRegistry[driver] = dialectable
}

//GetDatastoreDialectable returns DatastoreDialect for passed in driver.
func GetDatastoreDialectable(driver string) DatastoreDialect {
	if result, ok := datastoreDialectableRegistry[driver]; ok {
		return result
	}
	panic("Failed to lookup datastore dialectabe: " + driver)
}

func init() {
	RegisterDatastoreDialectable("mysql", &mySQLDialect{})
	RegisterDatastoreDialectable("pg", &pgDialect{})
	RegisterDatastoreDialectable("ora", &oraDialect{})
	RegisterDatastoreDialectable("mssql", &msSQLDialect{})
	RegisterDatastoreDialectable("sqlite3", &sqlLiteDialect{})
	RegisterDatastoreDialectable("ndjson", &fileDialect{})
}
