package dsc

import "fmt"

var managerFactories = make(map[string]ManagerFactory)

func init() {
	var managerFactory ManagerFactory = &sqlManagerFactory{}
	sqlDrivers := []string{"mysql", "ora", "pg", "mssql", "sqlite3", "odbc"}
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

//GetManagerFactory returns a manager factory for passed in driver, or error.
func GetManagerFactory(driver string) (ManagerFactory, error) {
	if result, ok := managerFactories[driver]; ok {
		return result, nil
	}
	return nil, fmt.Errorf("Failed to lookup manager factory for '%v' ", driver)
}
