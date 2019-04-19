package dsc

import (
	"log"
	"time"
)

//AbstractConnection represents an abstract connection
type AbstractConnection struct {
	Connection
	lastUsed       *time.Time
	config         *Config
	connectionPool chan Connection
}

//Config returns a datastore config
func (ac *AbstractConnection) Config() *Config {
	return ac.config
}

//ConnectionPool returns a connection channel
func (ac *AbstractConnection) ConnectionPool() chan Connection {
	return ac.connectionPool
}

//LastUsed returns a last used time
func (ac *AbstractConnection) LastUsed() *time.Time {
	return ac.lastUsed
}

//SetLastUsed sets last used time
func (ac *AbstractConnection) SetLastUsed(ts *time.Time) {
	ac.lastUsed = ts
}

//Close closes connection if pool is full or send it back to the pool
func (ac *AbstractConnection) Close() error {
	channel := ac.Connection.ConnectionPool()
	config := ac.config
	if len(ac.Connection.ConnectionPool()) < config.MaxPoolSize {
		var connection = ac.Connection
		channel <- connection
		var ts = time.Now()
		connection.SetLastUsed(&ts)

	} else {
		return ac.Connection.CloseNow()
	}
	return nil
}

//Begin starts a transaction  - this method is an abstract method
func (ac *AbstractConnection) Begin() error { return nil }

//Commit finishes current transaction  - this method is an abstract method
func (ac *AbstractConnection) Commit() error { return nil }

//Rollback - discards transaction  - this method is an abstract method
func (ac *AbstractConnection) Rollback() error { return nil }

//NewAbstractConnection create a new abstract connection
func NewAbstractConnection(config *Config, connectionPool chan Connection, connection Connection) *AbstractConnection {
	return &AbstractConnection{config: config, connectionPool: connectionPool, Connection: connection}
}

//AbstractConnectionProvider represents an abstract/superclass ConnectionProvider
type AbstractConnectionProvider struct {
	ConnectionProvider
	config         *Config
	connectionPool chan Connection
}

//Config returns a datastore config,
func (cp *AbstractConnectionProvider) Config() *Config {
	return cp.config
}

//ConnectionPool returns a ConnectionPool
func (cp *AbstractConnectionProvider) ConnectionPool() chan Connection {
	return cp.connectionPool
}

//SpawnConnectionIfNeeded creates a new connection if connection pool has not reached size controlled by Config.PoolSize
func (cp *AbstractConnectionProvider) SpawnConnectionIfNeeded() {
	config := cp.ConnectionProvider.Config()
	if config.PoolSize == 0 {
		config.PoolSize = 1
	}
	connectionPool := cp.ConnectionProvider.ConnectionPool()
	for i := len(connectionPool); i < config.PoolSize; i++ {
		connection, err := cp.ConnectionProvider.NewConnection()
		if err != nil {
			log.Printf("failed to create connection %v\n", err)
			break
		}

		select {
		case <-time.After(1 * time.Second):
			log.Fatalf("failed to add connection to queue (size: %v, cap:%v)", len(connectionPool), cap(connectionPool))
		case connectionPool <- connection:
		}

	}
}

//Close closes a datastore connection or returns it to the pool (Config.PoolSize and Config.MaxPoolSize).
func (cp *AbstractConnectionProvider) Close() error {
	for i := 0; i < len(cp.connectionPool); i++ {
		var connection Connection
		select {
		case <-time.After(1 * time.Second):
		case connection = <-cp.connectionPool:
			err := connection.CloseNow()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

//Get returns a new datastore connection or error.
func (cp *AbstractConnectionProvider) Get() (Connection, error) {
	cp.ConnectionProvider.SpawnConnectionIfNeeded()
	connectionPool := cp.ConnectionProvider.ConnectionPool()

	var result Connection
	select {
	case <-time.After(100 * time.Millisecond):
		{
			Logf("unable to acquire connection from pool, creating new connection ...")
		}
	case result = <-connectionPool:
	}
	if result == nil {
		var err error
		result, err = cp.ConnectionProvider.NewConnection()
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

//NewAbstractConnectionProvider create a new AbstractConnectionProvider
func NewAbstractConnectionProvider(config *Config, connectionPool chan Connection, connectionProvider ConnectionProvider) *AbstractConnectionProvider {
	return &AbstractConnectionProvider{config: config, connectionPool: connectionPool, ConnectionProvider: connectionProvider}
}
