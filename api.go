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
/*

This library provies capabilities to connection to SQL and noSQL datastores, providing sql layer on top of it.

For native database/sql it is just a ("database/sql") proxy, and for noSQL it supports simple SQL that is being translated to
put/get,scan,batch native NoSQL operations.


Datastore Manager implements read, persist (no insert nor update), and delete operations.
Read operation requires data record mapper,
Persist operation requires dml provider.
Delete operation requries key provider.

Datastore Manager provides default record mapper and dml/key provider for a struct, if no actual implementation is passed in.

The following tags can be used
1 column - name of datastore field/column
2 autoincrement - boolean flag to use autoincrement, in this case on insert the value can be automatically set back on application model class
3 primaryKey - boolean flag primary key
4 dateLayout - date layout check string to time.Time conversion
4 dateFormat - date format check java simple date format
5 sequence - name of sequence used to generate next id
6 transient - boolean flag to not map a field with record data



Usage:

	type Interest struct {
		Id int	`autoincrement:"true"`
		Name string
		ExpiryTimeInSecond int `column:"expiry"`
		Category string
	}

 	manager := factory.CreateFromURL("file:///etc/mystore-config.json")
        interest := Interest{}

    	intersts = make([]Interest, 0)
        err:= manager.ReadAll(&interests, SELECT id, name, expiry, category FROM interests", nil ,nil)
    	if err != nil {
        	panic(err.Error())
    	}
    	...

	inserted, updated, err:= manager.PersistAll(&intersts, "interests", nil)
	if err != nil {
        	panic(err.Error())
   	}

*/
package dsc

import "database/sql"

//Scanner represents a datastrore data scanner. This abstraction provides ability to converting and assigning datastore record of data to provided destination
type Scanner interface {

	//Returns all column specified in select statement.
	Columns() ([]string, error)

	//Scans datastore record data to convert and assign it to provided destinations, a destination needs to be pointer.
	Scan(dest ...interface{}) error
}

//RecordMapper represents a datastore record mapper, it is responsible for mapping data record into application abstraction.
type RecordMapper interface {

	//Maps data record by passing to the scanner references to the application abstraction
	Map(scanner Scanner) (interface{}, error)
}

//ParametrizedSQL represents a parmetrized SQL with its binding values, in order of occurrence.
type ParametrizedSQL struct {
	SQL    string        //Sql
	Values []interface{} //binding parameter values
}

//DmlProvider represnet dml generator, which is responsible for providing parametrized sql, it takes operation type:
//SqlTypeInsert = 0
//SqlTypeUpdate = 1
//SqlTypeDelete = 2
// and instance to the application abstraction
type DmlProvider interface {
	Get(operationType int, instance interface{}) *ParametrizedSQL

	KeySetter
	KeyGetter
}

//Manager represents  datastore manager.
type Manager interface {
	Config() *Config

	//ConnectionProvider returns connection provider
	ConnectionProvider() ConnectionProvider

	//Execute executes provided sql, with the arguments, '?' is used as placeholder for and arguments
	Execute(sql string, parameters ...interface{}) (sql.Result, error)

	//ExecuteAll executes all provided sql
	ExecuteAll(sqls []string) ([]sql.Result, error)

	//ExecuteOnConnection executes sql on passed in connection, this allowes to maintain transaction if supported
	ExecuteOnConnection(connection Connection, sql string, parameters []interface{}) (sql.Result, error)

	//ExecuteAllOnConnection executes all sql on passed in connection, this allowes to maintain transaction if supported
	ExecuteAllOnConnection(connection Connection, sqls []string) ([]sql.Result, error)

	//ReadSingle fetches a single record of data, it takes pointer to the result, sql query, binding parameters, record to application instance mapper
	ReadSingle(resultPointer interface{}, query string, parameters []interface{}, mapper RecordMapper) (success bool, err error)

	//ReadSingleOnConnection fetches a single record of data on connection, it takes connection, pointer to the result, sql query, binding parameters, record to application instance mapper
	ReadSingleOnConnection(connection Connection, resultPointer interface{}, query string, parameters []interface{}, mapper RecordMapper) (success bool, err error)

	//ReadAll reads all records, it takes pointer to the result slice , sql query, binding parameters, record to application instance mapper
	ReadAll(resultSlicePointer interface{}, query string, parameters []interface{}, mapper RecordMapper) error

	//ReadAllOnConnection reads all records, it takes connection, pointer to the result slice , sql query, binding parameters, record to application instance mapper
	ReadAllOnConnection(connection Connection, resultSlicePointer interface{}, query string, parameters []interface{}, mapper RecordMapper) error

	//ReadAllWithHandler reads data for passed in query and parameters, for each row reading handler will be called, to continue reading next row it needs to return true
	ReadAllWithHandler(query string, parameters []interface{}, readingHandler func(scanner Scanner) (toContinue bool, err error)) error

	//ReadAllOnWithHandlerOnConnection reads data for passed in query and parameters, on connection,  for each row reading handler will be called, to continue reading next row, it needs to return true
	ReadAllOnWithHandlerOnConnection(connection Connection, query string, parameters []interface{}, readingHandler func(scanner Scanner) (toContinue bool, err error)) error

	//ReadAllOnWithHandlerOnConnection persists all passed in data to the table, it uses dml provider to generate DML for each row.
	PersistAll(slicePointer interface{}, table string, provider DmlProvider) (inserted int, updated int, err error)

	//PersistAllOnConnection persists all passed in data on connection to the table, it uses dml provider to generate DML for each row.
	PersistAllOnConnection(connection Connection, dataPointer interface{}, table string, provider DmlProvider) (inserted int, updated int, err error)

	//PersistSingle persists single row into table, it uses dml provider to generate DML to the row.
	PersistSingle(dataPointer interface{}, table string, provider DmlProvider) (inserted int, updated int, err error)

	//PersistSingleOnConnection persists single row on connection into table, it uses dml provider to generate DML to the row.
	PersistSingleOnConnection(connection Connection, dataPointer interface{}, table string, provider DmlProvider) (inserted int, updated int, err error)

	//connection persists all all row of data to passed in table, it uses key setter to optionally set back autoincrement value, and func to generate parametrized sql for the row.
	PersistData(connection Connection, data []interface{}, table string, keySetter KeySetter, sqlProvider func(item interface{}) *ParametrizedSQL) (int, error)

	//DeleteAll deletes all record for passed in slice pointer from table, it uses key provider to take id/key for the record.
	DeleteAll(slicePointer interface{}, table string, keyProvider KeyGetter) (deleted int, err error)

	//DeleteAllOnConnection deletes all record on connection for passed in slice pointer from table, it uses key provider to take id/key for the record.
	DeleteAllOnConnection(connection Connection, resultPointer interface{}, table string, keyProvider KeyGetter) (deleted int, err error)

	//DeleteSingle deletes single row of data from table, it uses key provider to take id/key for the record.
	DeleteSingle(resultPointer interface{}, table string, keyProvider KeyGetter) (success bool, err error)

	//DeleteSingleOnConnection deletes single row of data on connection  from table, it uses key provider to take id/key for the record.
	DeleteSingleOnConnection(connection Connection, resultPointer interface{}, table string, keyProvider KeyGetter) (success bool, err error)

	//ClassifyDataAsInsertableOrUpdatable classifies records are inserable and what are updatable.
	ClassifyDataAsInsertableOrUpdatable(connection Connection, slicePointer interface{}, table string, provider DmlProvider) (insertables, updatables []interface{}, err error)

	//TableDescriptorRegistry returns Table Descriptor Registry
	TableDescriptorRegistry() TableDescriptorRegistry
}

//DatastoreDialect represents datastore dialects.
type DatastoreDialect interface {
	GetDatastores(manager Manager) ([]string, error)

	GetTables(manager Manager, datastore string) ([]string, error)

	DropTable(manager Manager, datastore string, table string) error

	CreateTable(manager Manager, datastore string, table string, options string) error

	CanCreateDatastore(manager Manager) bool

	CreateDatastore(manager Manager, datastore string) error

	CanDropDatastore(manager Manager) bool

	DropDatastore(manager Manager, datastore string) error

	GetCurrentDatastore(manager Manager) (string, error)

	GetSequence(manager Manager, name string) (int64, error)

	//Flag if data store can persist batch
	CanPersistBatch() bool
}

//TransactionManager represents a transaction manager.
type TransactionManager interface {
	Begin() error

	Commit() error

	Rollback() error
}

//Connection  represents a datastore  connection
type Connection interface {
	Config() *Config

	ConnectionPool() chan Connection

	//Close closes connection or it returns it back to the pool
	Close() error

	CloseNow() error //closes connecition, it does not return it back to the pool

	Unwrap(target interface{}) interface{}

	TransactionManager
}

//ConnectionProvider represents a datastore connection provider.
type ConnectionProvider interface {
	Get() (Connection, error)

	Config() *Config

	ConnectionPool() chan Connection

	SpawnConnectionIfNeeded()

	NewConnection() (Connection, error)

	Close() error
}

//ManagerFactory represents a manager factory.
type ManagerFactory interface {

	//Creates manager, takes config pointer.
	Create(config *Config) (Manager, error)

	//Creates manager from url, can url needs to point to Config JSON.
	CreateFromURL(url string) (Manager, error)
}

//ManagerRegistry represents  a manager registry.
type ManagerRegistry interface {
	Get(name string) Manager

	Register(name string, manager Manager)
}

//KeySetter represents id/key mutator.
type KeySetter interface {

	//SetKey sets autoincrement/sql value to the application domain instance.
	SetKey(instance interface{}, seq int64)
}

//KeyGetter represents id/key accessor.
type KeyGetter interface {

	//Key returns key/id for the the application domain instance.
	Key(instance interface{}) []interface{}
}

//TableDescriptor represents a table details.
type TableDescriptor struct {
	Table         string
	Autoincrement bool
	PkColumns     []string
	Columns       []string
	Schema        []map[string]interface{} //Schema to be interpreted by NoSQL drivers for create table operation .
	SchemaURL     string                   //url with JSON to the TableDescriptor.Schema.
}

//TableDescriptorRegistry represents a registry to store table descriptors by table name.
type TableDescriptorRegistry interface {

	//Has checks if descriptor is defined for the table.
	Has(table string) bool

	//Get returns a table descriptor for passed in table, it calls panic if descriptor is not found, to avoid it please always use Has check.
	Get(table string) *TableDescriptor

	//Register registers a table descriptor.
	Register(descriptor *TableDescriptor)

	//Tables returns all registered tables.
	Tables() []string
}
