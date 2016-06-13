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
package dsc

import (
	"log"
	"time"
)



//AbstractConnection represents an abstract connection
type AbstractConnection struct {
	config         *Config
	connectionPool chan Connection
	connection     Connection
}

//Config returns a datastore config
func (ac *AbstractConnection) Config() *Config {
	return ac.config
}

//ConnectionPool returns a connection channel
func (ac *AbstractConnection) ConnectionPool() chan Connection {
	return ac.connectionPool
}


//Close closes connection if pool if full or send it back to the pool
func (ac *AbstractConnection) Close() (error) {
	channel :=ac.connection.ConnectionPool()
	config := ac.config
	if (len(ac.connection.ConnectionPool())  < config.MaxPoolSize) {
		var connection = ac.connection
		channel <- connection;
	} else {
		return ac.connection.CloseNow()
	}
	return nil
}


//CloseNow abstract method - panic
func (ac *AbstractConnection) CloseNow() error {
	panic("CloseNow is abstract method please provide implementation in the sub class")
}

//Unwrap abstract method - panic
func (ac *AbstractConnection) Unwrap(target interface{}) interface{} {
	panic("Unwrap is abstract method please provide implementation in the sub class")
}

//Begin abstract method - panic
func (ac *AbstractConnection) Begin() (error) {
	panic("Begin is abstract method please provide implementation in the sub class")
}

//Commit abstract method - panic
func (ac *AbstractConnection) Commit() (error) {
	panic("Commit is abstract method please provide implementation in the sub class")

}

//Rollback abstract method - panic
func (ac *AbstractConnection) Rollback() (error) {
	panic("Rollback is abstract method please provide implementation in the sub class")
}

//NewAbstractConnection create a new abstract connection
func NewAbstractConnection(config *Config, connectionPool chan Connection, self Connection) AbstractConnection{
	return  AbstractConnection{config:config, connectionPool:connectionPool, connection:self}
}




//AbstractConnectionProvider represents an abstract/superclass ConnectionProvider
type AbstractConnectionProvider struct {
	config         *Config
	connectionPool chan Connection
	Provider       ConnectionProvider
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
	config := cp.Provider.Config()
	if config.PoolSize == 0 {
		config.PoolSize = 1
	}
	connectionPool := cp.Provider.ConnectionPool()
	for i := len(connectionPool); i < config.PoolSize; i++ {
		connection, err := cp.Provider.NewConnection()
		if (err != nil) {
			log.Printf("Failed to create connection %v\n", err)
			break
		}

		select {
		case <-time.After(1 * time.Second):
			log.Fatalf("Failed to add connection to queue (size: %v, cap:%v)", len(connectionPool), cap(connectionPool))
		case connectionPool <- connection:
		}

	}
}

//NewConnection creates a new connection or error.
func (cp *AbstractConnectionProvider) NewConnection() (Connection, error) {
	panic("This is abstract method please provide implementation in the sub class")
}

//Close closes a datastore connection or returns it to the pool (Config.PoolSize and Config.MaxPoolSize).
func (cp *AbstractConnectionProvider) Close() (error) {
	for i:=0 ;i<len(cp.connectionPool);i++ {
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
	cp.Provider.SpawnConnectionIfNeeded();
	connectionPool := cp.Provider.ConnectionPool()

	var result Connection
	select {
		case <-time.After(1 * time.Second): {
			log.Printf("Failed to acquire connection from pool after a second, creating new connection ...")
		}
		case result = <-connectionPool:
	}
	if (result == nil) {
		var err error
		result, err = cp.Provider.NewConnection()
		if (err != nil) {
			return nil, err
		}
	}
	return result, nil
}

//NewAbstractConnectionProvider create a new AbstractConnectionProvider
func NewAbstractConnectionProvider(config *Config,connectionPool chan Connection,self ConnectionProvider) AbstractConnectionProvider {
	return AbstractConnectionProvider{config:config, connectionPool:connectionPool, Provider:self}
}
