package dsc_test

import (
	"testing"
	"github.com/viant/dsc"
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
)

func TestBetweenPredicate(t *testing.T) {

	predicate:=dsc.NewBetweenPredicate(10, 20)

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
		predicate := dsc.NewComparablePredicate( ">", "1")
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
	parameters := toolbox.NewSliceIterator([]string{"abc%"})
	predicate, err := dsc.NewSQLCriterionPredicate(dsc.SQLCriterion{Operator:"Like", RightOperand:"?"}, parameters)
	assert.Nil(t, err)
	{
		assert.True(t, predicate.Apply("ABC"))
		assert.False(t, predicate.Apply("AB"))

	}
}

func TestNewCriteriaPredicate(t *testing.T) {

	parameters:=[]interface{}{"abc%", 123}
	iterator := toolbox.NewSliceIterator(parameters)
	predicate, err := dsc.NewSQLCriteriaPredicate(iterator,
		dsc.SQLCriterion{LeftOperand:"column1", Operator:"Like", RightOperand:"?", LogicalOperator:"or", },
		dsc.SQLCriterion{LeftOperand:"column2", Operator:"=", RightOperand:"?"},
	)
	assert.Nil(t, err)
	{
		assert.False(t, predicate.Apply(
			map[string]interface{}{
				"column1":"AB",
				"column2":12,
			},
		))
		assert.True(t, predicate.Apply(
			map[string]interface{}{
				"column1":"ABc",
				"column2":12,
			},
		))
		assert.True(t, predicate.Apply(
			map[string]interface{}{
				"column1":"AB",
				"column2":123,
			},
		))
	}
}