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





type sqlDatastoreDialect struct {

}

//CanDropDatastore returns true if this dialect can create datastore
func (d sqlDatastoreDialect)  CanCreateDatastore(manager Manager) bool {
	return true
}

//CanDropDatastore returns true if this dialect can drop datastore
func (d sqlDatastoreDialect)  CanDropDatastore(manager Manager) bool {
	return true
}


//CreateDatastore create a new datastore (database/schema), it takes manager and target datastore
func (d sqlDatastoreDialect) CreateDatastore(manager Manager, datastore string) (error) {
	_, err := manager.Execute("CREATE DATABASE " + datastore)
	return err
}


//DropTable drops a datastore (database/schema), it takes manager and datastore to be droped
func (d sqlDatastoreDialect) DropDatastore(manager Manager, datastore string) (error) {
	_, err := manager.Execute("DROP DATABASE " + datastore)
	return err
}

//DropTable drops a table in datastore managed by manager.
func (d sqlDatastoreDialect) DropTable(manager Manager, datastore string, table string) (error) {
	_, err := manager.Execute("DROP TABLE" + datastore + "." + table)
	return err
}

//CreateTable creates table on in datastore managed by manager.
func (d sqlDatastoreDialect)  CreateTable(manager Manager, datastore string, table string, options string) (error) {
	_, err := manager.Execute("CREATE TABLE " + datastore + "." + table + "(" + options +")")
	return err
}

//GetTables return tables names for passed in datastore managed by manager.
func (d sqlDatastoreDialect) GetTables(manager Manager, datastore string) ([]string, error) {
	var rows = make([]nameRecord, 0)
	err := manager.ReadAll(&rows, "SELECT table_name AS name FROM  information_schema.tables WHERE table_schema = ?", []interface{}{datastore}, nil)
	if (err != nil) {
		return nil, err
	}
	var result = make([]string, len(rows))
	for _, row := range rows {
		result = append(result, row.Name)
	}
	return result, nil
}

//GetDatastores returns name of datastores, takes  manager as parameter
func (d sqlDatastoreDialect) GetDatastores(manager Manager) ([] string, error) {
	var rows = make([]nameRecord, 0)
	err := manager.ReadAll(&rows, "SELECT schema_name AS name FROM  information_schema.schemata", nil, nil)
	if (err != nil) {
		return nil, err
	}
	var result = make([]string, len(rows))
	for _, row := range rows {
		result = append(result, row.Name)
	}
	return result, nil
}

//CanPersistBatch return true if datastore can persist in batch
func (d sqlDatastoreDialect) CanPersistBatch() bool {
	return false
}


type mySQLDialect struct {
	sqlDatastoreDialect
}

type nameRecord struct {
	Name string `column:"name"`
}



func (d mySQLDialect) GetCurrentDatastore(manager Manager) (string, error) {
	var result = make([]nameRecord, 0)
	success, err := manager.ReadSingle(&result, "SELECT DATABASE() AS name", nil, nil)
	if (err != nil || ! success) {
		return "", err
	}
	return result[0].Name, nil

}


func (d mySQLDialect) GetSequence(manager Manager, name string) (int64, error) {
	var result = make([]int64, 0)
	success, err := manager.ReadSingle(&result, "SELECT auto_increment FROM information_schema.tables WHERE table_name = ? AND table_schema = DATABASE()", []interface{}{name}, nil)
	if (err != nil || ! success) {
		return 0, err
	}
	return result[0], nil
}


type pgDialect struct {
	sqlDatastoreDialect
}


func (d pgDialect) GetCurrentDatastore(manager Manager) (string, error) {
	var result = make([]nameRecord, 0)
	success, err := manager.ReadSingle(&result, "SELECT current_schema() AS name", nil, nil)
	if (err != nil || ! success) {
		return "", err
	}
	return result[0].Name, nil

}

func (d pgDialect) GetSequence(manager Manager, name string) (int64, error) {
	var result = make([]int64, 0)
	success, err := manager.ReadSingle(&result, fmt.Sprintf("SELECT currval(%v)", name), nil, nil)
	if (err != nil || ! success) {
		return 0, err
	}
	return result[0], nil
}



type oraDialect struct {
	sqlDatastoreDialect
}

func (d oraDialect) GetTables(manager Manager, datastore string) ([]string, error) {
	var rows = make([]nameRecord, 0)
	err := manager.ReadAll(&rows, "SELECT table_name AS name  FROM all_tables WHERE owner = ?", []interface{}{datastore}, nil)
	if (err != nil) {
		return nil, err
	}
	var result = make([]string, len(rows))
	for _, row := range rows {
		result = append(result, row.Name)
	}
	return result, nil
}


func (d oraDialect) GetCurrentDatastore(manager Manager) (string, error) {
	var result = make([]nameRecord, 0)
	success, err := manager.ReadSingle(&result, "SELECT sys_context( 'userenv', 'current_schema' ) AS name FROM dual", nil, nil)
	if (err != nil || ! success) {
		return "", err
	}
	return result[0].Name, nil

}


func (d oraDialect) GetDatastores(manager Manager) ([] string, error) {
	var rows = make([]nameRecord, 0)
	err := manager.ReadAll(&rows, "SELECT username AS name FROM all_users", nil, nil)
	if (err != nil) {
		return nil, err
	}
	var result = make([]string, len(rows))
	for _, row := range rows {
		result = append(result, row.Name)
	}
	return result, nil
}



func (d oraDialect) GetSequence(manager Manager, name string) (int64, error) {
	var result = make([]int64, 0)
	success, err := manager.ReadSingle(&result, fmt.Sprintf("SELECT %v.nextval from dual", name), nil, nil)
	if (err != nil || ! success) {
		return 0, err
	}
	return result[0], nil
}




type msSQLDialect struct {
	sqlDatastoreDialect
}


func (d msSQLDialect) GetCurrentDatastore(manager Manager) (string, error) {
	var result = make([]nameRecord, 0)
	success, err := manager.ReadSingle(&result, "SELECT SCHEMA_NAME() AS name", nil, nil)
	if (err != nil || ! success) {
		return "", err
	}
	return result[0].Name, nil

}


func (d msSQLDialect) GetSequence(manager Manager, name string) (int64, error) {
	var result = make([]int64, 0)
	success, err := manager.ReadSingle(&result, fmt.Sprintf("SELECT current_value FROM sys.sequences WHERE  name = '%v'", name), nil, nil)
	if (err != nil || ! success) {
		return 0, err
	}
	return result[0], nil
}

