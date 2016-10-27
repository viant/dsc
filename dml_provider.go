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
	toolbox.AssertKind(instance, reflect.Struct, "instance")
	var reflectable = reflect.ValueOf(instance)
	return p.dmlBuilder.GetParametrizedSQL(sqlType, func(column string) interface{} {
		return p.readValue(reflectable, column)
	})
}

func newMetaDmlProvider(table string, targetType reflect.Type) DmlProvider {
	descriptor := NewTableDescriptor(table, targetType)
	dmlBuilder := NewDmlBuilder(descriptor)
	return &metaDmlProvider{dmlBuilder: dmlBuilder,
		columnToFieldNameMap: toolbox.NewFieldSettingByKey(targetType, "column")}
}

//NewDmlProviderIfNeeded returns a new NewDmlProvider for a table and target type if passed provider was nil.
func NewDmlProviderIfNeeded(provider DmlProvider, table string, targetType reflect.Type) DmlProvider {
	if provider != nil {
		return provider
	}
	var result DmlProvider
	result = newMetaDmlProvider(table, targetType)
	return result
}

//NewKeyGetterIfNeeded returns a new ketter if passedin keyGetter was nil for the target type
func NewKeyGetterIfNeeded(keyGetter KeyGetter, table string, targetType reflect.Type) KeyGetter {
	if keyGetter != nil {
		return keyGetter
	}
	var result KeyGetter
	result = newMetaDmlProvider(table, targetType)
	return result
}
