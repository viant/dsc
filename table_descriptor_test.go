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
	"time"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
)


type User1 struct {
	Name        string `column:"name"`
	DateOfBirth time.Time `column:"date" dateFormat:"2006-01-02 15:04:05.000000"`
	Id          int 	`autoincrement:"true"`
	Other       string `transient:"true"`
}


func TestDataset(t *testing.T) {

	descriptor := dsc.NewTableDescriptor("users", (*User1)(nil))
	assert.Equal(t, "users", descriptor.Table)
	assert.Equal(t, "Id", descriptor.PkColumns[0])
	assert.Equal(t, true,  descriptor.Autoincrement)
	assert.Equal(t, 3,  len(descriptor.Columns))

}