package dsc

import "errors"

type fileConnection struct {
	*AbstractConnection
	URL string
	ext string
}

func (fc *fileConnection) Close() error {
	return nil
}

func (fc *fileConnection) Unwrap(target interface{}) interface{} {
	return errors.New("unsupported")
}

type fileConnectionProvider struct {
	*AbstractConnectionProvider
}

func (cp *fileConnectionProvider) NewConnection() (Connection, error) {
	config := cp.Config()
	url := config.Get("url")
	ext := config.Get("ext")
	var fileConnection = &fileConnection{URL: url, ext: ext}
	var connection = fileConnection
	var super = NewAbstractConnection(config, cp.ConnectionProvider.ConnectionPool(), connection)
	fileConnection.AbstractConnection = super
	return connection, nil
}

func newFileConnectionProvider(config *Config) ConnectionProvider {
	if config.MaxPoolSize == 0 {
		config.MaxPoolSize = 1
	}
	fileConnectionProvider := &fileConnectionProvider{}
	var connectionProvider ConnectionProvider = fileConnectionProvider
	super := NewAbstractConnectionProvider(config, make(chan Connection, config.MaxPoolSize), connectionProvider)
	fileConnectionProvider.AbstractConnectionProvider = super
	return connectionProvider
}
