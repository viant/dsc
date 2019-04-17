package dsc_test

import (
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"testing"
)

func TestSqlAllSqlDialect(t *testing.T) {
	var dialect dsc.DatastoreDialect
	for _, driver := range []string{"sqlite3", "mysql", "pg", "ora", "mssql"} {
		dialect = dsc.GetDatastoreDialect(driver)
		assert.NotNil(t, dialect)
	}
}
func TestSqlDialect(t *testing.T) {

	dialect := dsc.GetDatastoreDialect("sqlite3")
	config := dsc.NewConfig("sqlite3", "[url]", "url:./test/bar.db")
	factory := dsc.NewManagerFactory()
	manager, err := factory.Create(config)
	assert.Nil(t, err)

	assert.Nil(t, dialect.CreateDatastore(manager, "bar"))
	assert.Nil(t, dialect.DropDatastore(manager, "bar"))

	assert.Nil(t, dialect.CreateTable(manager, "bar", "table1", "`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,`username` varchar(255) DEFAULT NULL,`active` tinyint(1) DEFAULT '1',`salary` decimal(7,2) DEFAULT NULL,`comments` text,`last_access_time` timestamp DEFAULT CURRENT_TIMESTAMP"))
	assert.Nil(t, dialect.CreateTable(manager, "bar", "table2", "`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,`username` varchar(255) DEFAULT NULL,`active` tinyint(1) DEFAULT '1',`salary` decimal(7,2) DEFAULT NULL,`comments` text,`last_access_time` timestamp DEFAULT CURRENT_TIMESTAMP"))
	tables, err := dialect.GetTables(manager, "bar")
	assert.Nil(t, err)
	assert.Equal(t, []string{"table1", "table2"}, tables)

	seq, err := dialect.GetSequence(manager, "table1")
	assert.Nil(t, err)
	assert.EqualValues(t, 1, seq)

	columns, err := dialect.GetColumns(manager, "bar.db", "table1")
	assert.Nil(t, err)
	assert.EqualValues(t, 6, len(columns))

	pk := dialect.GetKeyName(manager, "bar.db", "table1")
	assert.Nil(t, err)
	assert.EqualValues(t, "id", pk)

	ddl, err := dialect.ShowCreateTable(manager, "table1")
	assert.Nil(t, err)

	assert.EqualValues(t, `CREATE TABLE table1(
	id INTEGER PRIMARY KEY ,
	username varchar(255),
	active tinyint(1),
	salary decimal(7,2),
	comments text,
	last_access_time timestamp);`, ddl)

	assert.Nil(t, dialect.DropTable(manager, "bar", "table1"))
	assert.False(t, dialect.CanPersistBatch())
	datastore, err := dialect.GetCurrentDatastore(manager)
	assert.Equal(t, "bar.db", datastore)
	datastores, err := dialect.GetDatastores(manager)
	assert.Equal(t, []string{"bar.db"}, datastores)

	assert.Nil(t, dialect.DropDatastore(manager, "bar"))

	{
		mySQLDialect := dsc.GetDatastoreDialect("mysql")
		assert.True(t, mySQLDialect.CanPersistBatch())
	}
}
