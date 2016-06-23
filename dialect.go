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

// Package dsc - Datastore dialect
package dsc

import "errors"

var errUnsupportedOperation = errors.New("Unsupported operation")

type defaultDialect struct{}

func (d defaultDialect) GetDatastores(manager Manager) ([]string, error) {
	return nil, nil
}

func (d defaultDialect) GetTables(manager Manager, datastore string) ([]string, error) {
	return nil, nil
}

func (d defaultDialect) DropTable(manager Manager, datastore string, table string) error {
	return nil
}

func (d defaultDialect) CreateTable(manager Manager, datastore string, table string, options string) error {
	return nil
}

func (d defaultDialect) CanCreateDatastore(manager Manager) bool {
	return false
}

func (d defaultDialect) CreateDatastore(manager Manager, datastore string) error {
	return errUnsupportedOperation
}

func (d defaultDialect) CanDropDatastore(manager Manager) bool {
	return false
}

func (d defaultDialect) DropDatastore(manager Manager, datastore string) error {
	return errUnsupportedOperation
}

func (d defaultDialect) GetCurrentDatastore(manager Manager) (string, error) {
	return "", nil
}

func (d defaultDialect) GetSequence(manager Manager, name string) (int64, error) {
	return 0, errUnsupportedOperation
}

func (d defaultDialect) CanPersistBatch() bool {
	return false
}

//NewDefaultDialect crates a defulat dialect. DefaultDialect can be used as a embeddable struct (super class).
func NewDefaultDialect() DatastoreDialect {
	return &defaultDialect{}
}
