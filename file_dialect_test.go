/*
 *
 *
 * Copyright 2012-2016 Viant.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
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

func TestFileDialect(t *testing.T) {
	config := dsc.NewConfig("ndjson", "[url]", "dateFormat:yyyy-MM-dd hh:mm:ss,ext:json,url:"+dsunit.ExpandTestProtocolIfNeeded("test:///test/"))
	manager, err := dsc.NewManagerFactory().Create(config)
	assert.Nil(t, err)
	dialect := dsc.GetDatastoreDialectable("ndjson")
	tables, err := dialect.GetTables(manager, "")
	assert.Nil(t, err)
	assert.Equal(t, 4, len(tables))

	name, err := dialect.GetCurrentDatastore(manager)
	assert.Equal(t, dsunit.ExpandTestProtocolIfNeeded("test:///test/"), name)
	assert.False(t, dialect.CanCreateDatastore(manager))
	assert.False(t, dialect.CanDropDatastore(manager))
	assert.False(t, dialect.CanPersistBatch())
	_, err = dialect.GetDatastores(manager)
	assert.Nil(t, err)
	err = dialect.DropDatastore(manager, "abc")
	assert.NotNil(t, err, "Unsupported")
	err = dialect.CreateDatastore(manager, "abc")
	assert.NotNil(t, err, "Unsupported")

	_, err = dialect.GetSequence(manager, "abc")
	assert.NotNil(t, err, "Unsupported")

}
