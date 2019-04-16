package dsc

import (
	"database/sql"
	"fmt"
	"strings"
)

var managerFactories = make(map[string]ManagerFactory)

func init() {
	var managerFactory ManagerFactory = &sqlManagerFactory{}
	sqlDrivers := []string{"mysql", "ora", "pg", "postgres", "mssql", "sqlite3", "odbc", "cql", "oci8"}
	for _, driver := range sqlDrivers {
		RegisterManagerFactory(driver, managerFactory)
	}
	RegisterManagerFactory("ndjson", &jsonFileManagerFactory{})
	RegisterManagerFactory("csv", &delimiteredFileManagerFactory{","})
	RegisterManagerFactory("tsv", &delimiteredFileManagerFactory{"\t"})
}

//RegisterManagerFactory registers manager factory for passed in driver.
func RegisterManagerFactory(driver string, factory ManagerFactory) {
	managerFactories[driver] = factory
}

func isSQLDatabase(driver string) bool {
	_, err := sql.Open(driver, "")
	if err == nil {
		return true
	}
	return !strings.Contains(err.Error(), "unknown driver")
}

//GetManagerFactory returns a manager factory for passed in driver, or error.
func GetManagerFactory(driver string) (ManagerFactory, error) {
	if result, ok := managerFactories[driver]; ok {
		return result, nil
	}

	if isSQLDatabase(driver) {
		var managerFactory ManagerFactory = &sqlManagerFactory{}
		RegisterManagerFactory(driver, managerFactory)
		return managerFactory, nil
	}
	return nil, fmt.Errorf("failed to lookup manager factory for '%v' ", driver)
}
