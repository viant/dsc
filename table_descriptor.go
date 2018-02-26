package dsc

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/viant/toolbox"
	"strings"
)

type commonTableDescriptorRegistry struct {
	sync.RWMutex
	manager  Manager
	registry map[string]*TableDescriptor
}

func (r *commonTableDescriptorRegistry) Has(table string) bool {
	r.RLock()
	defer r.RUnlock()
	_, found := r.registry[table]
	return found
}

func (r *commonTableDescriptorRegistry) getDescriptor(table string) *TableDescriptor {
	dbConfig := r.manager.Config()
	dialect := GetDatastoreDialect(dbConfig.DriverName)
	datastore, _ := dialect.GetCurrentDatastore(r.manager)
	key := dialect.GetKeyName(r.manager, datastore, table)
	isAutoincrement := dialect.IsAutoincrement(r.manager, datastore, table)
	descriptor := &TableDescriptor{
		Table:         table,
		Autoincrement: isAutoincrement,
		PkColumns:     []string{},
	}
	if key != "" {
		descriptor.PkColumns = strings.Split(key, ",")
	}
	return descriptor
}

func (r *commonTableDescriptorRegistry) Get(table string) *TableDescriptor {
	r.RLock()
	if descriptor, found := r.registry[table]; found {
		r.RUnlock()
		return descriptor
	}
	r.RUnlock()
	var result = r.getDescriptor(table)
	r.Register(result)
	return result
}

func (r *commonTableDescriptorRegistry) Register(descriptor *TableDescriptor) error {
	if descriptor.Table == "" {
		return fmt.Errorf("table name was not set %v", descriptor)
	}
	for i, column := range descriptor.Columns {
		if column == "" {
			return fmt.Errorf("columns[%d] was empty %v %v", i, descriptor.Table, descriptor.Columns)
		}
	}
	for i, column := range descriptor.PkColumns {
		if column == "" {
			return fmt.Errorf("pkColumns[%d] was empty %v %v", i, descriptor.Table, descriptor.Columns)
		}
	}
	r.RLock()
	defer r.RUnlock()
	r.registry[descriptor.Table] = descriptor
	return nil
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

func newTableDescriptorRegistry() *commonTableDescriptorRegistry {
	return &commonTableDescriptorRegistry{registry: make(map[string]*TableDescriptor)}
}

//newTableDescriptorRegistry returns a new newTableDescriptorRegistry
func NewTableDescriptorRegistry() TableDescriptorRegistry {
	return newTableDescriptorRegistry()
}

//HasSchema check if table desciptor has defined schema.
func (d *TableDescriptor) HasSchema() bool {
	return len(d.SchemaUrl) > 0 || d.Schema != nil
}

//NewTableDescriptor creates a new table descriptor for passed in instance, it can use the following tags:"column", "dateLayout","dateFormat", "autoincrement", "primaryKey", "sequence", "transient"
func NewTableDescriptor(table string, instance interface{}) (*TableDescriptor, error) {
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
			if !toolbox.HasSliceAnyElements(pkColumns, column) {
				pkColumns = append(pkColumns, column)
			}
			continue
		}
		if key == "id" {
			if !toolbox.HasSliceAnyElements(pkColumns, column) {
				pkColumns = append(pkColumns, column)
			}
			continue
		}
	}
	if len(pkColumns) == 0 {
		return nil, fmt.Errorf("No primary key defined on table: %v, type: %v, consider adding 'primaryKey' tag to primary key column", table, targetType)
	}
	return &TableDescriptor{
		Table:         table,
		Autoincrement: autoincrement,
		Columns:       columns,
		PkColumns:     pkColumns,
	}, nil
}
