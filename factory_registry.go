/*
 *
 *
 * Copyright 2012-2016 Viant.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *
 */
package dsc

import "fmt"

var managerFactories = make(map[string]ManagerFactory)

func init() {
	var managerFactory ManagerFactory = &sqlManagerFactory{}
	sqlDrivers := []string{"mysql", "ora", "pg", "mssql", "sqlite3"}
	for _, driver := range sqlDrivers {
		RegisterManagerFactory(driver, managerFactory)
	}
	RegisterManagerFactory("ndjson", &jsonFileManagerFactory{})
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
