package dsc_test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"testing"
)

func TestConfigHasDateLayout(t *testing.T) {
	{
		config := dsc.NewConfig("sqlite3", "[url]", "url:./test/foo.db")
		assert.False(t, config.HasDateLayout())
	}

	{
		config := dsc.NewConfig("sqlite3", "[url]", "url:./test/foo.db,dateFormat:yyyy-MM-dd hh:mm:ss")
		assert.True(t, config.HasDateLayout())
	}

}

func TestConfigHasParameterLayout(t *testing.T) {
	{
		config := dsc.NewConfig("sqlite3", "[url]", "url:./test/foo.db")
		assert.False(t, config.Has("url1"))
	}

	{
		config := dsc.NewConfig("sqlite3", "[url]", "url:./test/foo.db,dateFormat:yyyy-MM-dd hh:mm:ss")
		assert.True(t, config.Has("url"))
	}

}

func TestGetPanic(t *testing.T) {
	config := dsc.NewConfig("sqlite3", "[url]", "url:./test/foo.db")

	defer func() {
		if err := recover(); err != nil {
			expected := "Missing value in descriptor abc"
			actual := fmt.Sprintf("%v", err)
			assert.Equal(t, actual, expected, "Assert Kind")
		}
	}()

	config.Get("abc")

}
