package dsc_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

func TestBetweenPredicate(t *testing.T) {

	predicate := dsc.NewBetweenPredicate(10, 20)

	assert.False(t, predicate.Apply(9))
	assert.True(t, predicate.Apply(10))
	assert.True(t, predicate.Apply(11))
	assert.False(t, predicate.Apply(21))

}

func TestInPredicate(t *testing.T) {

	{
		predicate := dsc.NewInPredicate("10", 20, "a")
		assert.False(t, predicate.Apply(9))
		assert.True(t, predicate.Apply(10))
		assert.False(t, predicate.Apply(15))
		assert.True(t, predicate.Apply("a"))
		assert.True(t, predicate.Apply(20))
		assert.False(t, predicate.Apply(21))
	}
}

func TestComparablePredicate(t *testing.T) {
	{
		predicate := dsc.NewComparablePredicate(">", "1")
		assert.True(t, predicate.Apply(3))
		assert.False(t, predicate.Apply(1))
	}
	{
		predicate := dsc.NewComparablePredicate("<", "1")
		assert.True(t, predicate.Apply(0))
		assert.False(t, predicate.Apply(3))
	}
	{
		predicate := dsc.NewComparablePredicate("!=", "1")
		assert.True(t, predicate.Apply(0))
		assert.False(t, predicate.Apply(1))
	}

}

func TestNewLikePredicate(t *testing.T) {
	{
		predicate := dsc.NewLikePredicate("abc%efg")
		assert.False(t, predicate.Apply("abefg"))
		assert.True(t, predicate.Apply("abcefg"))

	}
	{
		predicate := dsc.NewLikePredicate("abc%")
		assert.True(t, predicate.Apply("abcfg"))

	}
}

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

func TestNewComparablePredicate(t *testing.T) {

	{
		predicate := dsc.NewComparablePredicate("=", "abc")
		assert.True(t, predicate.Apply("abc"))
		assert.False(t, predicate.Apply("abdc"))
	}
	{
		predicate := dsc.NewComparablePredicate("!=", "abc")
		assert.True(t, predicate.Apply("abcc"))
		assert.False(t, predicate.Apply("abc"))
	}

	{
		predicate := dsc.NewComparablePredicate(">=", 3)
		assert.True(t, predicate.Apply(10))
		assert.False(t, predicate.Apply(1))
	}

	{
		predicate := dsc.NewComparablePredicate("<=", 3)
		assert.True(t, predicate.Apply(1))
		assert.False(t, predicate.Apply(10))
	}

}

func TestNewInPredicate(t *testing.T) {
	predicate := dsc.NewInPredicate(1.2, 1.5)
	assert.True(t, predicate.Apply("1.2"))
	assert.False(t, predicate.Apply("1.1"))
}
