package dsc

import "errors"

var errUnsupportedOperation = errors.New("Unsupported operation")

type DefaultDialect struct{}

func (d DefaultDialect) GetDatastores(manager Manager) ([]string, error) {
	return nil, nil
}

func (d DefaultDialect) GetTables(manager Manager, datastore string) ([]string, error) {
	return nil, nil
}

func (d DefaultDialect) DropTable(manager Manager, datastore string, table string) error {
	return nil
}

func (d DefaultDialect) CreateTable(manager Manager, datastore string, table string, options string) error {
	return nil
}

func (d DefaultDialect) CanCreateDatastore(manager Manager) bool {
	return false
}

func (d DefaultDialect) CreateDatastore(manager Manager, datastore string) error {
	return errUnsupportedOperation
}

func (d DefaultDialect) CanDropDatastore(manager Manager) bool {
	return false
}

func (d DefaultDialect) DropDatastore(manager Manager, datastore string) error {
	return errUnsupportedOperation
}

func (d DefaultDialect) GetCurrentDatastore(manager Manager) (string, error) {
	return "", nil
}

func (d DefaultDialect) GetSequence(manager Manager, name string) (int64, error) {
	return 0, errUnsupportedOperation
}

func (d DefaultDialect) GetKeyName(manager Manager, datastore, table string) string {
	return ""
}

func (d DefaultDialect) IsAutoincrement(manager Manager, datastore, table string) bool {
	return false
}

func (d DefaultDialect) CanPersistBatch() bool {
	return false
}

//DisableForeignKeyCheck disables fk check
func (d DefaultDialect) DisableForeignKeyCheck(manager Manager, connection Connection) error {
	return nil
}

//DisableForeignKeyCheck disables fk check
func (d DefaultDialect) EnableForeignKeyCheck(manager Manager, connection Connection) error {
	return nil
}

//NewDefaultDialect crates a defulat dialect. DefaultDialect can be used as a embeddable struct (super class).
func NewDefaultDialect() DatastoreDialect {
	return &DefaultDialect{}
}
