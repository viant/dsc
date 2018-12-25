package dsc

import "reflect"

type TableColumn struct {
	ColumnName       string `column:"column_name"`
	DataType         string `column:"data_type"`
	DataTypeLength   *int64 `column:"data_type_length"`
	NumericPrecision *int64 `column:"numeric_precision"`
	NumericScale     *int64 `column:"numeric_scale"`
	IsNullable       *bool  `column:"is_nullable"`
	Position         int    `column:"position"`
	scanType         reflect.Type
}

func (c *TableColumn) Name() string {
	return c.ColumnName
}

func (c *TableColumn) Length() (length int64, ok bool) {
	if c.DataTypeLength == nil {
		return 0, false
	}
	return *c.DataTypeLength, true
}

func (c *TableColumn) DecimalSize() (precision, scale int64, ok bool) {
	if c.NumericPrecision == nil || c.NumericScale == nil {
		return 0, 0, false
	}
	return *c.NumericPrecision, *c.NumericScale, true
}

func (c *TableColumn) ScanType() reflect.Type {
	return c.scanType
}

func (c *TableColumn) Nullable() (nullable, ok bool) {
	if c.IsNullable == nil {
		return false, false
	}
	return *c.IsNullable, true
}

// Common type include "VARCHAR", "TEXT", "NVARCHAR", "DECIMAL", "BOOL", "INT", "BIGINT".
func (c *TableColumn) DatabaseTypeName() string {
	return c.DataType
}

//NewColumn create new TableColumn
func NewColumn(name, typeName string, length, precision, scale *int64, scanType reflect.Type, nullable *bool) Column {
	var result = &TableColumn{
		ColumnName:       name,
		DataType:         typeName,
		DataTypeLength:   length,
		NumericPrecision: precision,
		NumericScale:     scale,
		scanType:         scanType,
		IsNullable:       nullable,
	}
	return result
}

//NewSimpleColumn create simple TableColumn name
func NewSimpleColumn(name, typeName string) Column {
	return &TableColumn{
		ColumnName: name,
		DataType:   typeName,
	}
}
