package dsc

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTableDescriptor_From(t *testing.T) {

	var useCases = []struct {
		description string
		table       *TableDescriptor
		expect      string
	}{
		{
			description: "from table",
			table:       &TableDescriptor{Table: "table1"},
			expect:      "table1",
		},
		{
			description: "from query - default alias",
			table:       &TableDescriptor{Table: "table1", FromQuery: "SELECT * FROM table1"},
			expect:      "(SELECT * FROM table1) AS t",
		},
		{
			description: "from query with alias",
			table:       &TableDescriptor{Table: "table1", FromQuery: "SELECT * FROM table1", FromQueryAlias: "newTable"},
			expect:      "(SELECT * FROM table1) AS newTable",
		},
	}

	for _, useCase := range useCases {
		actual := useCase.table.From()
		assert.Equal(t, useCase.expect, actual, useCase.description)
	}
}
