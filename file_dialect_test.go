package dsc_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"testing"
)

func TestFileDialect(t *testing.T) {

	config := dsc.NewConfig("ndjson", "[url]", "dateFormat:yyyy-MM-dd hh:mm:ss,ext:json,url:test/")
	manager, err := dsc.NewManagerFactory().Create(config)
	assert.Nil(t, err)
	dialect := dsc.GetDatastoreDialect("ndjson")

	tables, err := dialect.GetTables(manager, "")
	assert.Nil(t, err)
	assert.True(t, len(tables) > 0)

	assert.False(t, dialect.CanCreateDatastore(manager))
	assert.False(t, dialect.CanDropDatastore(manager))
	assert.False(t, dialect.CanPersistBatch())
	_, err = dialect.GetDatastores(manager)
	assert.Nil(t, err)
	err = dialect.DropDatastore(manager, "abc")
	assert.NotNil(t, err, "Unsupported")
	err = dialect.CreateDatastore(manager, "abc")
	assert.NotNil(t, err, "Unsupported")

	_, err = dialect.GetSequence(manager, "abc")
	assert.NotNil(t, err, "Unsupported")

}
