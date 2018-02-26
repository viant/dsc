package dsc_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"testing"
)

func TestCreateFromURL(t *testing.T) {
	factory := dsc.NewManagerFactory()
	{
		_, err := factory.CreateFromURL("test/file_config3.json")
		assert.NotNil(t, err)
	}

	{
		_, err := factory.CreateFromURL("test/file_config.json")
		assert.NotNil(t, err)
	}
}

func TestMissingDricer(t *testing.T) {
	factory := dsc.NewManagerFactory()
	{
		_, err := factory.CreateFromURL("test/file_config3.json")
		assert.NotNil(t, err)
	}

}
