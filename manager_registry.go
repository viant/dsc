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

// Package dsc - Datastore Manager Registry
package dsc

import (
	"fmt"

	"github.com/viant/toolbox"
)

type commonManagerRegistry struct {
	registry map[string](Manager)
}

func (r commonManagerRegistry) Register(name string, manager Manager) {
	if previousManager, found := r.registry[name]; found {
		previousManager.ConnectionProvider().Close()
	}
	r.registry[name] = manager
}

func (r commonManagerRegistry) Get(name string) Manager {
	if result, ok := r.registry[name]; ok {
		return result
	}
	panic(fmt.Sprintf("Failed to lookup manager '%v', available names:%v", name, toolbox.MapKeysToStringSlice(r.registry)))
}

func (r commonManagerRegistry) Names() []string {
	return toolbox.MapKeysToStringSlice(r.registry)
}

//NewManagerRegistry create a new ManagerRegistry
func NewManagerRegistry() ManagerRegistry {
	var result = &commonManagerRegistry{registry: make(map[string](Manager))}
	return result
}
