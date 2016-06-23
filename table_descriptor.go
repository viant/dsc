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

// Package dsc - Table descriptor
package dsc

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/viant/toolbox"
)

type commonTableDescriptorRegistry struct {
	sync.RWMutex
	registry map[string]*TableDescriptor
}

func (r *commonTableDescriptorRegistry) Has(table string) bool {
	r.RLock()
	defer r.RUnlock()
	_, found := r.registry[table]
	return found
}

func (r *commonTableDescriptorRegistry) Get(table string) *TableDescriptor {
	r.RLock()
	defer r.RUnlock()
	if descriptor, found := r.registry[table]; found {
		return descriptor
	}
	panic("Failed to lookup table descriptor for " + table)
}

func (r *commonTableDescriptorRegistry) Register(descriptor *TableDescriptor) {
	if descriptor.Table == "" {
		panic(fmt.Sprintf("Table name was not set %v", descriptor))
	}
	r.RLock()
	defer r.RUnlock()
	r.registry[descriptor.Table] = descriptor
}

func (r *commonTableDescriptorRegistry) Tables() []string {
	r.RLock()
	defer r.RUnlock()
	var result = make([]string, 0)
	for key := range r.registry {
		result = append(result, key)
	}
	return result
}

//NewTableDescriptorRegistry returns a new NewTableDescriptorRegistry
func NewTableDescriptorRegistry() TableDescriptorRegistry {
	var result TableDescriptorRegistry = &commonTableDescriptorRegistry{registry: make(map[string]*TableDescriptor)}
	return result
}

//HasSchema check if table desciptor has defined schema.
func (d *TableDescriptor) HasSchema() bool {
	return len(d.SchemaURL) > 0 || d.Schema != nil
}

//NewTableDescriptor creates a new table descriptor for passed in instance, it can use the following tags:"column", "dateLayout","dateFormat", "autoincrement", "primaryKey", "sequence", "transient"
func NewTableDescriptor(table string, instance interface{}) *TableDescriptor {
	targetType := toolbox.DiscoverTypeByKind(instance, reflect.Struct)
	var autoincrement bool
	var pkColumns = make([]string, 0)
	var columns = make([]string, 0)
	columnToFieldMap := toolbox.NewFieldSettingByKey(targetType, "column")

	for key := range columnToFieldMap {
		mapping, _ := columnToFieldMap[key]
		column, ok := mapping["column"]
		if !ok {
			column = mapping["fieldName"]
		}
		if _, ok := mapping["autoincrement"]; ok {
			pkColumns = append(pkColumns, column)
			autoincrement = true
			continue
		}
	}

	for key := range columnToFieldMap {
		mapping, _ := columnToFieldMap[key]
		column, ok := mapping["column"]
		if !ok {
			column = mapping["fieldName"]
		}

		columns = append(columns, column)
		if _, ok := mapping["primaryKey"]; ok {
			if ! toolbox.HasSliceAnyElements(pkColumns, column) {
				pkColumns = append(pkColumns, column)
			}
			continue
		}
		if key == "id" {
			if ! toolbox.HasSliceAnyElements(pkColumns, column) {
				pkColumns = append(pkColumns, column)
			}
			continue
		}
	}
	if len(pkColumns) == 0 {
		panic(fmt.Sprintf("No primary key defined on table: %v, type: %v", table, targetType))
	}
	return &TableDescriptor{
		Table:         table,
		Autoincrement: autoincrement,
		Columns:       columns,
		PkColumns:     pkColumns,
	}
}
