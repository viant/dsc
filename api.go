package dsc

import (
	"database/sql"
	"reflect"
	"time"
)

//Scanner represents a datastore data scanner. This abstraction provides the ability to convert and assign datastore record of data to provided destination
type Scanner interface {
	//Returns all columns specified in select statement.
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
	Type   int
}

//DmlProvider represents dml generator, which is responsible for providing parametrized sql, it takes operation type:
//SqlTypeInsert = 0
//SqlTypeUpdate = 1
//SqlTypeDelete = 2
// and instance to the application abstraction
type DmlProvider interface {
	Get(operationType int, instance interface{}) *ParametrizedSQL

	KeySetter
	KeyGetter
}

//Manager represents datastore manager.
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

	CreateTable(manager Manager, datastore string, table string, specification string) error

	CanCreateDatastore(manager Manager) bool

	CreateDatastore(manager Manager, datastore string) error

	CanDropDatastore(manager Manager) bool

	DropDatastore(manager Manager, datastore string) error

	GetCurrentDatastore(manager Manager) (string, error)

	//GetSequence returns a sequence number
	GetSequence(manager Manager, name string) (int64, error)

	//GetKeyName returns a name of TableColumn name that is a key, or coma separated list if complex key
	GetKeyName(manager Manager, datastore, table string) string

	//GetColumns returns TableColumn info
	GetColumns(manager Manager, datastore, table string) ([]Column, error)

	//IsAutoincrement returns true if autoicrement
	IsAutoincrement(manager Manager, datastore, table string) bool

	//Flag if data store can persist batch
	CanPersistBatch() bool

	//DisableForeignKeyCheck disables fk check
	DisableForeignKeyCheck(manager Manager, connection Connection) error

	//EnableForeignKeyCheck disables fk check
	EnableForeignKeyCheck(manager Manager, connection Connection) error

	//Normalizes placeholders, by default dsc uses '?' for placeholder if some dialect use difference this method should take care of it
	NormalizePlaceholders(SQL string) string

	//EachTable iterate all current connection manager datastore tables
	EachTable(manager Manager, handler func(table string) error) error

	//Init initializes connection
	Init(manager Manager, connection Connection) error
}

//Column represents TableColumn type interface (compabible with *sql.ColumnType
type Column interface {
	// Name returns the name or alias of the TableColumn.
	Name() string

	// Length returns the TableColumn type length for variable length TableColumn types such
	// as text and binary field types. If the type length is unbounded the value will
	// be math.MaxInt64 (any database limits will still apply).
	// If the TableColumn type is not variable length, such as an int, or if not supported
	// by the driver ok is false.
	Length() (length int64, ok bool)

	// DecimalSize returns the scale and precision of a decimal type.
	// If not applicable or if not supported ok is false.
	DecimalSize() (precision, scale int64, ok bool)

	// ScanType returns a Go type suitable for scanning into using Rows.Scan.
	// If a driver does not support this property ScanType will return
	// the type of an empty interface.
	ScanType() reflect.Type

	// Nullable returns whether the TableColumn may be null.
	// If a driver does not support this property ok will be false.
	Nullable() (nullable, ok bool)

	// DatabaseTypeName returns the database system name of the TableColumn type. If an empty
	// string is returned the driver type name is not supported.
	// Consult your driver documentation for a list of driver data types. Length specifiers
	// are not included.
	// Common type include "VARCHAR", "TEXT", "NVARCHAR", "DECIMAL", "BOOL", "INT", "BIGINT".
	DatabaseTypeName() string
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

	LastUsed() *time.Time

	SetLastUsed(ts *time.Time)

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
	OrderColumns  []string
	Schema        []map[string]interface{} //Schema to be interpreted by NoSQL drivers for create table operation .
	SchemaURL     string                   //url with JSON to the TableDescriptor.Schema.
	FromQuery     string                   //If table is query base then specify FromQuery
}

//TableDescriptorRegistry represents a registry to store table descriptors by table name.
type TableDescriptorRegistry interface {
	//Has checks if descriptor is defined for the table.
	Has(table string) bool

	//Get returns a table descriptor for passed in table, it calls panic if descriptor is not found, to avoid it please always use Has check.
	Get(table string) *TableDescriptor

	//Register registers a table descriptor.
	Register(descriptor *TableDescriptor) error

	//Tables returns all registered tables.
	Tables() []string
}
