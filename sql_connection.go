package dsc

import (
	"database/sql"
	"fmt"
	"time"
)

type sqlConnection struct {
	*AbstractConnection
	db *sql.DB
	tx *sql.Tx
}

func (c *sqlConnection) CloseNow() error {
	db, err := asSQLDb(c.db)
	if err != nil {
		return err
	}
	return db.Close()
}

func (c *sqlConnection) Begin() error {
	db, err := asSQLDb(c.db)
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	c.tx = tx
	return nil
}

func (c *sqlConnection) Unwrap(target interface{}) interface{} {
	if target == sqlDbPointer {
		return c.db
	} else if target == sqlTxtPointer {
		return c.tx
	}
	panic(fmt.Sprintf("Unsupported target type %v", target))
}

func (c *sqlConnection) Commit() error {
	if c.tx == nil {
		return fmt.Errorf("No active transaction")
	}
	err := c.tx.Commit()
	c.tx = nil
	return err
}

func (c *sqlConnection) Rollback() error {
	if c.tx == nil {
		return fmt.Errorf("No active transaction")
	}
	err := c.tx.Rollback()
	c.tx = nil
	return err
}

type sqlConnectionProvider struct {
	*AbstractConnectionProvider
}

func (c *sqlConnectionProvider) NewConnection() (Connection, error) {
	config := c.ConnectionProvider.Config()
	db, err := sql.Open(config.DriverName, config.Descriptor)
	if err != nil {
		return nil, fmt.Errorf("Failed to open connection to %v on %v due to %v", config.DriverName, config.Descriptor, err)
	}
	var sqlConnection = &sqlConnection{db: db}
	var connection Connection = sqlConnection
	var super = NewAbstractConnection(config, c.ConnectionProvider.ConnectionPool(), connection)
	sqlConnection.AbstractConnection = super
	return connection, nil
}

func (c *sqlConnectionProvider) Get() (Connection, error) {
	result, err := c.AbstractConnectionProvider.Get()
	if err != nil {
		return nil, err
	}
	db, err := asSQLDb(result.Unwrap(sqlDbPointer))
	if err != nil {
		return nil, err
	}

	//TODO add to control this with config parameters
	//set to min to not have lingered connection
	db.SetConnMaxLifetime(1 * time.Second)

	err = db.Ping()
	if err != nil {
		return result, nil
	}
	result, err = c.NewConnection()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func newSQLConnectionProvider(config *Config) ConnectionProvider {
	if config.MaxPoolSize == 0 {
		config.MaxPoolSize = 1
	}
	sqlConnectionProvider := &sqlConnectionProvider{}
	var connectionProvider ConnectionProvider = sqlConnectionProvider
	super := NewAbstractConnectionProvider(config, make(chan Connection, config.MaxPoolSize), connectionProvider)
	sqlConnectionProvider.AbstractConnectionProvider = super
	return connectionProvider
}
