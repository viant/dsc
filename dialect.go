package dsc

import "errors"

var errUnsupportedOperation = errors.New("Unsupported operation")

type defaultDialect struct{}

func (d defaultDialect) GetDatastores(manager Manager) ([]string, error) {
	return nil, nil
}

func (d defaultDialect) GetTables(manager Manager, datastore string) ([]string, error) {
	return nil, nil
}

func (d defaultDialect) DropTable(manager Manager, datastore string, table string) error {
	return nil
}

func (d defaultDialect) CreateTable(manager Manager, datastore string, table string, options string) error {
	return nil
}

func (d defaultDialect) CanCreateDatastore(manager Manager) bool {
	return false
}

func (d defaultDialect) CreateDatastore(manager Manager, datastore string) error {
	return errUnsupportedOperation
}

func (d defaultDialect) CanDropDatastore(manager Manager) bool {
	return false
}

func (d defaultDialect) DropDatastore(manager Manager, datastore string) error {
	return errUnsupportedOperation
}

func (d defaultDialect) GetCurrentDatastore(manager Manager) (string, error) {
	return "", nil
}

func (d defaultDialect) GetSequence(manager Manager, name string) (int64, error) {
	return 0, errUnsupportedOperation
}

func (d defaultDialect) CanPersistBatch() bool {
	return false
}

//NewDefaultDialect crates a defulat dialect. DefaultDialect can be used as a embeddable struct (super class).
func NewDefaultDialect() DatastoreDialect {
	return &defaultDialect{}
}
