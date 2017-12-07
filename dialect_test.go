package dsc_test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"testing"
)

func TestDialect(t *testing.T) {
	dialect := dsc.NewDefaultDialect()
	var manager dsc.Manager

	{
		datastores, err := dialect.GetDatastores(manager)
		assert.Nil(t, datastores)
		assert.Nil(t, err)
	}

	{
		tables, err := dialect.GetTables(manager, "test")
		assert.Nil(t, tables)
		assert.Nil(t, err)
	}

	{
		err := dialect.DropTable(manager, "test", "test")
		assert.Nil(t, err)
	}

	{
		err := dialect.CreateTable(manager, "test", "test", "")
		assert.Nil(t, err)
	}

	{
		check := dialect.CanCreateDatastore(manager)
		assert.False(t, check)
	}

	{
		err := dialect.CreateDatastore(manager, "test")
		assert.NotNil(t, err)
	}

	{
		check := dialect.CanDropDatastore(manager)
		assert.False(t, check)
	}

	{
		err := dialect.DropDatastore(manager, "test")
		assert.NotNil(t, err)
	}

	{
		store, err := dialect.GetCurrentDatastore(manager)
		assert.Equal(t, "", store)
		assert.Nil(t, err)
	}
	{
		seq, err := dialect.GetSequence(manager, "test")
		assert.EqualValues(t, 0, seq)
		assert.NotNil(t, err)
	}

	{
		check := dialect.CanPersistBatch()
		assert.False(t, check)
	}

}

func TestDialectRegistry(t *testing.T) {
	dialect := dsc.GetDatastoreDialect("ndjson")
	assert.NotNil(t, dialect)

	defer func() {
		if err := recover(); err != nil {
			expected := "failed to lookup datastore dialectabe: test"
			actual := fmt.Sprintf("%v", err)
			assert.Equal(t, actual, expected, "Assert Kind")
		}
	}()
	dsc.GetDatastoreDialect("test")
}
