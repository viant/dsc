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
	"encoding/json"
	"fmt"

	"github.com/viant/toolbox"
)

type managerFactoryProxy struct{}

//Create creates a new manager for the passed in config.
func (f managerFactoryProxy) Create(config *Config) (Manager, error) {
	if config.DriverName == "" {
		return nil, fmt.Errorf("DriverName was empty %v", config)
	}
	factory, err := GetManagerFactory(config.DriverName)
	if err != nil {
		return nil, fmt.Errorf("Failed to lookup manager factory for `%v`, make sure you have imported required implmentation", config.DriverName)
	}
	return factory.Create(config)
}

//CreateFromURL create a new manager from URL, url resource should be a JSON Config
func (f managerFactoryProxy) CreateFromURL(url string) (Manager, error) {
	reader, _, err := toolbox.OpenReaderFromURL(url)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	config := &Config{}
	err = json.NewDecoder(reader).Decode(config)
	if err != nil {
		return nil, err
	}
	config.Init()
	return f.Create(config)
}

//NewManagerFactory create a new manager factory.
func NewManagerFactory() ManagerFactory {
	var manager ManagerFactory = &managerFactoryProxy{}
	return manager
}
