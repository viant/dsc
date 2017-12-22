package dsc

import (
	"errors"
	"fmt"
	"github.com/viant/toolbox"
	"os"
)

type fileConnection struct {
	*AbstractConnection
	URL   string
	ext   string
	files map[string]*os.File
}

func (fc *fileConnection) Close() error {
	for _, file := range fc.files {
		file.Close()
	}
	return nil
}

func getFile(filename string, connection Connection) (*os.File, error) {
	fileConn, ok := connection.(*fileConnection)
	if !ok {
		return nil, fmt.Errorf("invalid connection type")
	}
	var err error
	if _, ok := fileConn.files[filename]; !ok {
		if len(fileConn.files) == 0 {
			fileConn.files = make(map[string]*os.File)
		}
		if !toolbox.FileExists(filename) {
			fileConn.files[filename], err = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
		} else {
			fileConn.files[filename], err = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
		}
		if err != nil {
			return nil, err
		}
	}
	return fileConn.files[filename], nil

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
