package dsc_test

import (
	"testing"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"github.com/stretchr/testify/assert"
)

func TestCreateFromURL(t *testing.T) {
	factory := dsc.NewManagerFactory()
	{
		_, err := factory.CreateFromURL(dsunit.ExpandTestProtocolIfNeeded("test:///test/file_config3.json"))
		assert.NotNil(t, err)
	}
	{
		manager, err := factory.CreateFromURL(dsunit.ExpandTestProtocolIfNeeded("test:///test/file_config.json"))
		assert.Nil(t, err)
		assert.NotNil(t, manager)
	}
}
