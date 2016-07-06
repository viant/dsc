package dsc_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
)

func TestFileDialect(t *testing.T) {
	config := dsc.NewConfig("ndjson", "[url]", "dateFormat:yyyy-MM-dd hh:mm:ss,ext:json,url:"+dsunit.ExpandTestProtocolAsUrlIfNeeded("test:///test/"))
	manager, err := dsc.NewManagerFactory().Create(config)
	assert.Nil(t, err)
	dialect := dsc.GetDatastoreDialectable("ndjson")
	tables, err := dialect.GetTables(manager, "")
	assert.Nil(t, err)
	assert.Equal(t, 4, len(tables))

	name, err := dialect.GetCurrentDatastore(manager)
	assert.Equal(t, dsunit.ExpandTestProtocolAsUrlIfNeeded("test:///test/"), name)
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
