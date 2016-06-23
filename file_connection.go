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

// Package dsc - File connection
package dsc

import (
	"fmt"
)

type fileConnection struct {
	AbstractConnection
	URL string
	ext string
}

func (fc *fileConnection) Close() error {
	return nil
}

func (fc *fileConnection) Begin() error {
	return nil
}

func (fc *fileConnection) Unwrap(target interface{}) interface{} {
	panic(fmt.Sprintf("Unsupported target type %v", target))
}

func (fc *fileConnection) Commit() error { return nil }

func (fc *fileConnection) Rollback() error { return nil }

type fileConnectionProvider struct {
	AbstractConnectionProvider
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
