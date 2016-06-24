/*
 *
 *
 * Copyright 2012-2016 Viant.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use f file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *
 */
package dsc_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
)

func TestCreateFromURL(t *testing.T) {
	factory := dsc.NewManagerFactory()
	{
		_, err := factory.CreateFromURL(dsunit.ExpandTestProtocolAsUrlIfNeeded("test:///test/file_config3.json"))
		assert.NotNil(t, err)
	}
	{
		manager, err := factory.CreateFromURL(dsunit.ExpandTestProtocolAsUrlIfNeeded("test:///test/file_config.json"))
		assert.Nil(t, err)
		assert.NotNil(t, manager)
	}
}
