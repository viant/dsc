package dsc

import (
	"fmt"
	"strings"
)

var querySQLTemplate = "SELECT %v FROM %v WHERE %v"
var insertSQLTemplate = "INSERT INTO %v(%v) VALUES(%v)"
var updateSQLTemplate = "UPDATE %v SET %v WHERE %v"
var deleteSQLTemplate = "DELETE FROM %v WHERE %v"

// DmlBuilder represents a insert,update,delete statement builder.
type DmlBuilder struct {
	TableDescriptor *TableDescriptor
	NonPkColumns    *[]string
	Columns         *[]string
	InsertSQL       string
	UpdateSQL       string
	DeleteSQL       string
	reserved        *Reserved
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

// GetParametrizedSQL returns GetParametrizedSQL for passed in sqlType, and value provider.
func (b *DmlBuilder) GetParametrizedSQL(sqlType int, valueProvider func(column string) interface{}) *ParametrizedSQL {
	switch sqlType {
	case SQLTypeInsert:
		return &ParametrizedSQL{
			SQL:    b.InsertSQL,
			Values: b.readInsertValues(valueProvider),
			Type:   SQLTypeInsert,
		}

	case SQLTypeUpdate:
		return &ParametrizedSQL{
			SQL:    b.UpdateSQL,
			Values: b.readValues(*b.Columns, valueProvider),
			Type:   SQLTypeUpdate,
		}
	case SQLTypeDelete:
		return &ParametrizedSQL{
			SQL:    b.DeleteSQL,
			Values: b.readValues(b.TableDescriptor.PkColumns, valueProvider),
			Type:   SQLTypeDelete,
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

func buildInsertSQL(descriptor *TableDescriptor, columns []string, nonPkColumns []string, reserved *Reserved) string {
	var insertColumns []string
	var insertValues []string = make([]string, 0)
	if descriptor.Autoincrement {
		insertColumns = append(insertColumns, nonPkColumns...)
	} else {
		insertColumns = append(insertColumns, columns...)
	}
	for range insertColumns {
		insertValues = append(insertValues, "?")
	}
	// Prefer per-manager reserved when provided
	if reserved != nil {
		reserved.quoteIfReserved(insertColumns)
	} else {
		updateReserved(insertColumns)
	}
	return fmt.Sprintf(insertSQLTemplate, descriptor.Table, strings.Join(insertColumns, ","), strings.Join(insertValues, ","))
}

func buildUpdateSQL(descriptor *TableDescriptor, nonPkColumns []string, reserved *Reserved) string {
	if reserved != nil {
		reserved.quoteIfReserved(nonPkColumns)
	} else {
		updateReserved(nonPkColumns)
	}
	pk := append([]string{}, descriptor.PkColumns...)
	if reserved != nil {
		reserved.quoteIfReserved(pk)
	} else {
		updateReserved(pk)
	}
	return fmt.Sprintf(updateSQLTemplate, descriptor.Table, buildAssignValueSQL(nonPkColumns, ","), buildAssignValueSQL(pk, " AND "))
}

func buildDeleteSQL(descriptor *TableDescriptor, reserved *Reserved) string {
	pk := append([]string{}, descriptor.PkColumns...)
	if reserved != nil {
		reserved.quoteIfReserved(pk)
	} else {
		updateReserved(pk)
	}
	return fmt.Sprintf(deleteSQLTemplate, descriptor.Table, buildAssignValueSQL(pk, " AND "))
}

// NewDmlBuilder returns a new DmlBuilder for passed in table descriptor.
func NewDmlBuilder(descriptor *TableDescriptor) *DmlBuilder {
	pkMap := make(map[string]int)

	if len(descriptor.PkColumns) > 0 {
		for i, k := range descriptor.PkColumns {
			pkMap[strings.ToLower(k)] = i
		}
	}
	var nonPkColumns = make([]string, 0)
	for _, column := range descriptor.Columns {
		idx, ok := pkMap[strings.ToLower(column)]
		if ok { //update pk with right case
			descriptor.PkColumns[idx] = column
		} else {
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
		InsertSQL:       buildInsertSQL(descriptor, columns, nonPkColumns, nil),
		UpdateSQL:       buildUpdateSQL(descriptor, nonPkColumns, nil),
		DeleteSQL:       buildDeleteSQL(descriptor, nil),
	}
}

// RebuildWithReserved rebuilds SQL statements using provided reserved settings.
func (b *DmlBuilder) RebuildWithReserved(res *Reserved) {
	if b == nil {
		return
	}
	b.reserved = res
	cols := append([]string{}, (*b.Columns)...)
	nonPk := append([]string{}, (*b.NonPkColumns)...)
	b.InsertSQL = buildInsertSQL(b.TableDescriptor, cols, nonPk, res)
	b.UpdateSQL = buildUpdateSQL(b.TableDescriptor, nonPk, res)
	b.DeleteSQL = buildDeleteSQL(b.TableDescriptor, res)
}
