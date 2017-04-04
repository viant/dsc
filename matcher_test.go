package dsc

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMatchValue(t *testing.T) {
	matcher := valueMatcher{optionallyEnclosingChar: "'", terminatorChars: ", \n\t)"}
	assert.Equal(t, 4, matcher.Match("  ?23? ", 2))
	assert.Equal(t, 2, matcher.Match("  ?2)3? ", 2))
	assert.Equal(t, 7, matcher.Match("  'a2\\'4'a", 2))
}

func TestMatchValues(t *testing.T) {
	matcher := valuesMatcher{
		valuesGroupingBeginChar:         "(",
		valuesGroupingEndChar:           ")",
		valueSeparator:                  ",",
		valueOptionallyEnclosedWithChar: "'",
		valueTerminatorCharacters:       ", \n\t)"}
	assert.Equal(t, 9, matcher.Match("a(1, 2, 3)a", 1))
	assert.Equal(t, 17, matcher.Match("a('a', '(b)', 'd')", 1))
}
