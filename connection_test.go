package dsc_test

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"testing"
	"time"
)

func TestConnectionConfig(t *testing.T) {
	config := &dsc.Config{}
	connection := newTestConnection(config)
	assert.Equal(t, config, connection.Config())
	assert.Nil(t, connection.Begin())
	assert.Nil(t, connection.Commit())
	assert.Nil(t, connection.Rollback())

}

func TestConnectionProvider(t *testing.T) {
	{
		config := &dsc.Config{}
		provider := newTestConnectionProvider(config)
		connection, err := provider.Get()
		assert.Nil(t, err)
		assert.NotNil(t, connection)
		provider.Close()
	}
	{
		config := &dsc.Config{MaxPoolSize: 3, PoolSize: 2}
		provider := newTestConnectionProvider(config)
		for i := 0; i < 3; i++ {
			connection, err := provider.Get()
			assert.Nil(t, err)
			assert.NotNil(t, connection)
		}
		provider.Close()
	}

	{
		config := &dsc.Config{MaxPoolSize: 3, PoolSize: 2}
		provider := newTestConnectionProvider(config)
		provider.error = errors.New("Test error")

		_, err := provider.Get()
		assert.NotNil(t, err)

	}

	{
		config := &dsc.Config{MaxPoolSize: 3, PoolSize: 2}
		provider := newTestConnectionProvider(config)
		sleep := int(2 * time.Second)
		provider.sleep = &sleep

		connection, err := provider.Get()
		assert.Nil(t, err)
		assert.NotNil(t, connection)
	}

}

type testConnection struct {
	*dsc.AbstractConnection
}

func (t *testConnection) CloseNow() error {
	return nil
}

func newTestConnection(config *dsc.Config) dsc.Connection {

	if config.MaxPoolSize == 0 {
		config.MaxPoolSize = 1
	}
	testConnection := &testConnection{}
	connection := dsc.NewAbstractConnection(config, make(chan dsc.Connection, config.MaxPoolSize), testConnection)
	testConnection.AbstractConnection = connection
	return testConnection
}

type testConnectionProvider struct {
	*dsc.AbstractConnectionProvider
	sleep *int
	error error
}

func (cp *testConnectionProvider) NewConnection() (dsc.Connection, error) {
	if cp.sleep != nil {
		time.Sleep(time.Duration(*cp.sleep))
	}

	if cp.error != nil {
		return nil, cp.error
	}
	config := cp.Config()
	var testConnection = &testConnection{}
	var connection = testConnection
	var super = dsc.NewAbstractConnection(config, cp.ConnectionProvider.ConnectionPool(), connection)
	testConnection.AbstractConnection = super
	return connection, nil
}

func newTestConnectionProvider(config *dsc.Config) *testConnectionProvider {
	if config.MaxPoolSize == 0 {
		config.MaxPoolSize = 1
	}
	testConnectionProvider := &testConnectionProvider{}
	var connectionProvider = testConnectionProvider
	super := dsc.NewAbstractConnectionProvider(config, make(chan dsc.Connection, config.MaxPoolSize), connectionProvider)
	testConnectionProvider.AbstractConnectionProvider = super
	return connectionProvider
}
