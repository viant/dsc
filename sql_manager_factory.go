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

// Package dsc - SQL manager factory
package dsc

import "github.com/viant/toolbox"

type sqlManagerFactory struct{}

func (mf *sqlManagerFactory) Create(config *Config) (Manager, error) {
	var connectionProvider = newSQLConnectionProvider(config)
	sqlManager := &sqlManager{}
	var self Manager = sqlManager
	super := NewAbstractManager(config, connectionProvider, self)
	sqlManager.AbstractManager = super
	return self, nil
}

func (mf sqlManagerFactory) CreateFromURL(url string) (Manager, error) {
	reader, _, err := toolbox.OpenReaderFromURL(url)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	config := &Config{}
	return mf.Create(config)
}
