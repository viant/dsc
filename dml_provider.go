package dsc

import (
	"reflect"
	"strings"

	"github.com/viant/toolbox"
)

//metaDmlProvider represents tag mapping base dml provider.
type metaDmlProvider struct {
	dmlBuilder           *DmlBuilder
	columnToFieldNameMap map[string](map[string]string)
}

func (p *metaDmlProvider) pkColumns() []string {
	return p.dmlBuilder.TableDescriptor.PkColumns
}

//Key returns primary key values
func (p *metaDmlProvider) Key(instance interface{}) []interface{} {
	result := p.readValues(instance, p.pkColumns())
	return result
}

//SetKey sets a key on passed in instance pointer
func (p *metaDmlProvider) SetKey(instancePointer interface{}, seq int64) {
	toolbox.AssertPointerKind(instancePointer, reflect.Struct, "instance")
	key := p.pkColumns()[0]
	columnSetting := p.columnToFieldNameMap[strings.ToLower(key)]
	if field, found := columnSetting["fieldName"]; found {
		var reflectable = reflect.ValueOf(instancePointer)
		if reflectable.Kind() == reflect.Ptr {
			field := reflectable.Elem().FieldByName(field)
			field.SetInt(seq)
		}

	}
}

func (p *metaDmlProvider) readValues(instance interface{}, columns []string) []interface{} {
	var result = make([]interface{}, len(columns))
	var reflectable = reflect.ValueOf(instance)
	if reflectable.Kind() == reflect.Ptr {
		reflectable = reflectable.Elem()
	}
	for i, column := range columns {
		result[i] = p.readValue(reflectable, column)
	}
	return result
}

func (p *metaDmlProvider) mapValueIfNeeded(value interface{}, column string, columnSetting map[string]string) interface{} {
	if mapping, found := columnSetting["valueMap"]; found {
		stringValue := toolbox.AsString(value)
		reverseMapValue := toolbox.MakeReverseStringMap(mapping, ":", ",")
		if mappedValue, ok := reverseMapValue[stringValue]; ok {
			return mappedValue
		}
	}
	return value
}

func (p *metaDmlProvider) readValue(source reflect.Value, column string) interface{} {
	columnSetting := p.columnToFieldNameMap[strings.ToLower(column)]
	if fieldName, ok := columnSetting["fieldName"]; ok {
		field := source.FieldByName(fieldName)
		value := toolbox.UnwrapValue(&field)
		if toolbox.IsZero(field) {
			value = nil
		}
		return p.mapValueIfNeeded(value, column, columnSetting)
	}
	return nil
}

//Get returns a ParametrizedSQL for specified sqlType and target instance.
func (p *metaDmlProvider) Get(sqlType int, instance interface{}) *ParametrizedSQL {
	var reflectable = reflect.ValueOf(instance)
	if reflectable.Kind() == reflect.Ptr {
		reflectable = reflectable.Elem()
	}
	//toolbox.AssertKind(instance, reflect.Type, "instance")
	return p.dmlBuilder.GetParametrizedSQL(sqlType, func(column string) interface{} {
		return p.readValue(reflectable, column)
	})
}

func newMetaDmlProvider(table string, targetType reflect.Type) (DmlProvider, error) {
	descriptor, err := NewTableDescriptor(table, targetType)
	if err != nil {
		return nil, err
	}
	dmlBuilder := NewDmlBuilder(descriptor)
	return &metaDmlProvider{dmlBuilder: dmlBuilder,
		columnToFieldNameMap: toolbox.NewFieldSettingByKey(targetType, "column")}, nil
}

//NewDmlProviderIfNeeded returns a new NewDmlProvider for a table and target type if passed provider was nil.
func NewDmlProviderIfNeeded(provider DmlProvider, table string, targetType reflect.Type) (DmlProvider, error) {
	if provider != nil {
		return provider, nil
	}
	return newMetaDmlProvider(table, targetType)
}

//NewKeyGetterIfNeeded returns a new key getter if supplied keyGetter was nil for the target type
func NewKeyGetterIfNeeded(keyGetter KeyGetter, table string, targetType reflect.Type) (KeyGetter, error) {
	if keyGetter != nil {
		return keyGetter, nil
	}
	return newMetaDmlProvider(table, targetType)
}

type mapDmlProvider struct {
	tableDescriptor *TableDescriptor
	dmlBuilder      *DmlBuilder
}

func (p *mapDmlProvider) Key(instance interface{}) []interface{} {
	var record = toolbox.AsMap(instance)
	var result = make([]interface{}, len(p.tableDescriptor.PkColumns))
	for i, column := range p.tableDescriptor.PkColumns {
		result[i] = record[column]
	}
	return result
}

func (p *mapDmlProvider) SetKey(instance interface{}, seq int64) {
	var record = toolbox.AsMap(instance)
	record[p.tableDescriptor.PkColumns[0]] = seq
}

func (p *mapDmlProvider) Get(sqlType int, instance interface{}) *ParametrizedSQL {
	var record = toolbox.AsMap(instance)
	return p.dmlBuilder.GetParametrizedSQL(sqlType, func(column string) interface{} {
		return record[column]
	})
}

func NewMapDmlProvider(descriptor *TableDescriptor) DmlProvider {
	var result = &mapDmlProvider{
		tableDescriptor: descriptor,
		dmlBuilder:      NewDmlBuilder(descriptor),
	}
	return result
}
