package dsc_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

func TestNewCriterionPredicate(t *testing.T) {
	{ //Like case
		parameters := toolbox.NewSliceIterator([]string{"abc%"})
		predicate, err := dsc.NewSQLCriterionPredicate(&dsc.SQLCriterion{Operator: "Like", RightOperand: "?"}, parameters)
		assert.Nil(t, err)
		{
			assert.True(t, predicate.Apply("ABC"))
			assert.False(t, predicate.Apply("AB"))

		}
	}
	{ //between case
		parameters := toolbox.NewSliceIterator([]string{"1", "10"})
		predicate, err := dsc.NewSQLCriterionPredicate(&dsc.SQLCriterion{Operator: "BETWEEN", RightOperands: []interface{}{"?", "?"}}, parameters)
		assert.Nil(t, err)
		{
			assert.True(t, predicate.Apply(5))
			assert.False(t, predicate.Apply(12))

		}
	}

	{ //error no operator
		parameters := toolbox.NewSliceIterator([]string{})
		_, err := dsc.NewSQLCriterionPredicate(&dsc.SQLCriterion{Operator: "", RightOperands: []interface{}{"?"}}, parameters)
		assert.NotNil(t, err)
	}

	{ //between error case
		parameters := toolbox.NewSliceIterator([]string{"1"})
		_, err := dsc.NewSQLCriterionPredicate(&dsc.SQLCriterion{Operator: "BETWEEN", RightOperands: []interface{}{"?", "?"}}, parameters)
		assert.NotNil(t, err)
	}
	{ //like error case
		parameters := toolbox.NewSliceIterator([]string{})
		_, err := dsc.NewSQLCriterionPredicate(&dsc.SQLCriterion{Operator: "Like", RightOperand: "?"}, parameters)
		assert.NotNil(t, err)
	}
	{ //in error case
		parameters := toolbox.NewSliceIterator([]string{})
		_, err := dsc.NewSQLCriterionPredicate(&dsc.SQLCriterion{Operator: "in", RightOperands: []interface{}{"?"}}, parameters)
		assert.NotNil(t, err)
	}
	{ //default error case
		parameters := toolbox.NewSliceIterator([]string{})
		_, err := dsc.NewSQLCriterionPredicate(&dsc.SQLCriterion{Operator: "=", RightOperand: "?"}, parameters)
		assert.NotNil(t, err)
	}

}

func TestNewCriteriaPredicate(t *testing.T) {

	parameters := []interface{}{"abc%", 123}
	iterator := toolbox.NewSliceIterator(parameters)
	predicate, err := dsc.NewSQLCriteriaPredicate(iterator,
		&dsc.SQLCriterion{LeftOperand: "column1", Operator: "Like", RightOperand: "?", LogicalOperator: "or"},
		&dsc.SQLCriterion{LeftOperand: "column2", Operator: "=", RightOperand: "?"},
	)
	assert.Nil(t, err)
	{
		assert.False(t, predicate.Apply(
			map[string]interface{}{
				"column1": "AB",
				"column2": 12,
			},
		))
		assert.True(t, predicate.Apply(
			map[string]interface{}{
				"column1": "ABc",
				"column2": 12,
			},
		))
		assert.True(t, predicate.Apply(
			map[string]interface{}{
				"column1": "AB",
				"column2": 123,
			},
		))
	}
}
