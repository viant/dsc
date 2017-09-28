package dsc_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
)

type User1 struct {
	Name        string    `column:"name"`
	DateOfBirth time.Time `column:"date" dateFormat:"2006-01-02 15:04:05.000000"`
	Id          int       `autoincrement:"true"`
	Other       string    `transient:"true"`
}

func TestTableDescriptor(t *testing.T) {

	descriptor := dsc.NewTableDescriptor("users", (*User1)(nil))
	assert.Equal(t, "users", descriptor.Table)
	assert.Equal(t, "Id", descriptor.PkColumns[0])
	assert.Equal(t, true, descriptor.Autoincrement)
	assert.Equal(t, 3, len(descriptor.Columns))

	assert.False(t, descriptor.HasSchema())

}
func TestTableDescriptorRegistry(t *testing.T) {
	descriptor := dsc.NewTableDescriptor("users", (*User1)(nil))
	registry := dsc.newTableDescriptorRegistry()
	assert.False(t, registry.Has("users"))
	registry.Register(descriptor)
	assert.True(t, registry.Has("users"))
	assert.Equal(t, []string{"users"}, registry.Tables())
}
