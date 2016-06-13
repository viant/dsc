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
	"fmt"
	"strings"

	"github.com/viant/toolbox"
)

var querySQLTemplate = "SELECT %v FROM %v WHERE %v"
var insertSQLTemplate = "INSERT INTO %v(%v) VALUES(%v)"
var updateSQLTemplate = "UPDATE %v SET %v WHERE %v"
var deleteSQLTemplate = "DELETE FROM %v WHERE %v"

//DmlBuilder represents a insert,update,delete statement builder.
type DmlBuilder struct {
	TableDescriptor *TableDescriptor
	NonPkColumns    *[]string
	Columns         *[]string
	InsertSQL       string
	UpdateSQL       string
	DeleteSQL       string
}

func (b *DmlBuilder) readValues(columns []string, valueProvider func(column string) interface{}) []interface{} {
	var result = make([]interface{}, len(columns))
	for i, column := range columns {
		result[i] = valueProvider(column)
	}
	return result
}

func (b *DmlBuilder) readInsertValues(valueProvider func(column string) interface{}) []interface{} {
	var columns []string
	if b.TableDescriptor.Autoincrement {
		columns = *b.NonPkColumns
	} else {
		columns = *b.Columns
	}
	return b.readValues(columns, valueProvider)
}

//GetParametrizedSQL returns GetParametrizedSQL for passed in sqlType, and value provider.
func (b *DmlBuilder) GetParametrizedSQL(sqlType int, valueProvider func(column string) interface{}) *ParametrizedSQL {
	switch sqlType {
	case SQLTypeInsert:
		return &ParametrizedSQL{
			SQL:    b.InsertSQL,
			Values: b.readInsertValues(valueProvider),
		}

	case SQLTypeUpdate:
		return &ParametrizedSQL{
			SQL:    b.UpdateSQL,
			Values: b.readValues(*b.Columns, valueProvider),
		}
	case SQLTypeDelete:
		return &ParametrizedSQL{
			SQL:    b.DeleteSQL,
			Values: b.readValues(b.TableDescriptor.PkColumns, valueProvider),
		}
	}
	panic(fmt.Sprintf("Unsupprted sqltype:%v", sqlType))
}

func buildAssignValueSQL(columns []string, separator string) string {
	result := ""
	for _, column := range columns {
		if len(result) > 0 {
			result = result + separator
		}
		result = result + " " + column + " = ?"
	}
	return result
}

func buildInsertSQL(descriptor *TableDescriptor, columns []string, nonPkColumns []string) string {
	var insertColumns []string
	if descriptor.Autoincrement {
		insertColumns = nonPkColumns
	} else {
		insertColumns = columns
	}
	insertValues := strings.Repeat("?,", len(insertColumns))
	insertValues = insertValues[0 : len(insertValues)-1] //removes last coma
	return fmt.Sprintf(insertSQLTemplate, descriptor.Table, strings.Join(insertColumns, ","), insertValues)
}

func buildUpdateSQL(descriptor *TableDescriptor, nonPkColumns []string) string {
	return fmt.Sprintf(updateSQLTemplate, descriptor.Table, buildAssignValueSQL(nonPkColumns, ","), buildAssignValueSQL(descriptor.PkColumns, " AND "))
}

func buildDeleteSQL(descriptor *TableDescriptor) string {
	return fmt.Sprintf(deleteSQLTemplate, descriptor.Table, buildAssignValueSQL(descriptor.PkColumns, " AND "))
}

//NewDmlBuilder returns a new DmlBuilder for passed in table descriptor.
func NewDmlBuilder(descriptor *TableDescriptor) *DmlBuilder {
	pkMap := make(map[string]bool)

	toolbox.SliceToMap(descriptor.PkColumns, pkMap, toolbox.CopyStringValueProvider, toolbox.TrueValueProvider)
	var nonPkColumns = make([]string, 0)
	for _, column := range descriptor.Columns {
		if _, ok := pkMap[column]; !ok {
			nonPkColumns = append(nonPkColumns, column)
		}
	}

	var columns = make([]string, 0)
	columns = append(columns, nonPkColumns...)
	columns = append(columns, descriptor.PkColumns...)
	return &DmlBuilder{
		TableDescriptor: descriptor,
		NonPkColumns:    &nonPkColumns,
		Columns:         &columns,
		InsertSQL:       buildInsertSQL(descriptor, columns, nonPkColumns),
		UpdateSQL:       buildUpdateSQL(descriptor, nonPkColumns),
		DeleteSQL:       buildDeleteSQL(descriptor),
	}
}
