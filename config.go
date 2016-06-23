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

// Package dsc - Datastore config
package dsc

import (
	"strings"

	"github.com/viant/toolbox"
)

//Config represent datastore config.
type Config struct {
	DriverName        string
	PoolSize          int
	MaxPoolSize       int
	Descriptor        string
	EncodedParameters string
	Parameters        map[string]string
}

//Get returns value for passed in parameter name or panic - please use Config.Has to check if value is present.
func (c *Config) Get(name string) string {
	if result, ok := c.Parameters[name]; ok {
		return result
	}
	panic("Missing value in descriptor " + name)
}

//HasDateLayout returns true if config has date layout, it checks dateLayout or dateFormat parameter names.
func (c *Config) HasDateLayout() bool {
	return toolbox.HasTimeLayout(c.Parameters)
}

//GetDateLayout returns date layout
func (c *Config) GetDateLayout() string {
	return toolbox.GetTimeLayout(c.Parameters)
}

//Has returns true if parameter with passed in name is present, otherwise it returns false.
func (c *Config) Has(name string) bool {
	if _, ok := c.Parameters[name]; ok {
		return true
	}
	return false
}

//Init makes parameter map from encoded parameters if presents, expands descriptor with parameter value using [param_name] matching pattern.
func (c *Config) Init() {
	if len(c.EncodedParameters) > 0 && len(c.Parameters) == 0 {
		c.Parameters = toolbox.MakeStringMap(c.EncodedParameters, ":", ",")
	}
	descriptor := c.Descriptor
	for key := range c.Parameters {
		macro := "[" + key + "]"
		descriptor = strings.Replace(descriptor, macro, c.Parameters[key], 1)
	}
	c.Descriptor = descriptor
}

//NewConfig creates new Config, it takes the following parameters
// descriptor - optional datastore connection string with macros that will be looked epxanded from for instance [user]:[password]@[url]
// encodedParameters should be in the following format:   <key1>:<value1>, ...,<keyN>:<valueN>
func NewConfig(driverName string, descriptor string, encodedParameters string) *Config {
	result := &Config{DriverName: driverName, PoolSize: 1, MaxPoolSize: 2, Descriptor: descriptor, EncodedParameters: encodedParameters, Parameters: make(map[string]string)}
	result.Init()
	return result
}
