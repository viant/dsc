package dsc

import "fmt"

//TableDescriptor represents a table details.
type TableDescriptor struct {
	Table          string
	Autoincrement  bool
	PkColumns      []string
	Columns        []string
	OrderColumns   []string
	Schema         []map[string]interface{} //Schema to be interpreted by NoSQL drivers for create table operation .
	SchemaURL      string                   //url with JSON to the TableDescriptor.Schema.
	FromQuery      string                   //If table is query base then specify FromQuery
	FromQueryAlias string
}

func (t *TableDescriptor) From() string {
	if t.FromQuery != "" {
		if t.FromQueryAlias == "" {
			t.FromQueryAlias = "t"
		}
		return fmt.Sprintf("(%v) AS %v", t.FromQuery, t.FromQueryAlias)
	}
	return t.Table
}

//TableDescriptorRegistry represents a registry to store table descriptors by table name.
type TableDescriptorRegistry interface {
	//Has checks if descriptor is defined for the table.
	Has(table string) bool

	//Get returns a table descriptor for passed in table, it calls panic if descriptor is not found, to avoid it please always use Has check.
	Get(table string) *TableDescriptor

	//Register registers a table descriptor.
	Register(descriptor *TableDescriptor) error

	//Tables returns all registered tables.
	Tables() []string
}
