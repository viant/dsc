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

func TestDataset(t *testing.T) {

	descriptor := dsc.NewTableDescriptor("users", (*User1)(nil))
	assert.Equal(t, "users", descriptor.Table)
	assert.Equal(t, "Id", descriptor.PkColumns[0])
	assert.Equal(t, true, descriptor.Autoincrement)
	assert.Equal(t, 3, len(descriptor.Columns))

}
