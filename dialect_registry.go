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
	RegisterDatastoreDialectable("ndjson", &fileDialect{})
}
