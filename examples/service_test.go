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

// Package examples -
package examples_test

import (
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/dsc"
	"github.com/viant/dsc/examples"
	"github.com/viant/dsunit"
)

func init() {
	go func() {
		examples.StartServer(dsunit.ExpandTestProtocolIfNeeded("test://test/config/store.json"), "8084")
	}()
	time.Sleep(2 * time.Second)
}

func getServices() ([]examples.InterestService, error) {
	local, err := examples.NewInterestService(dsunit.ExpandTestProtocolIfNeeded("test://test/config/store.json"))
	if err != nil {
		return nil, err
	}
	client := examples.NewInterestServiceClient("127.0.0.1:8084")
	return []examples.InterestService{local, client}, nil

}

func TestRead(t *testing.T) {
	if dsunit.SkipTestIfNeeded(t) {
		return
	}
	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromURL(t, "test://test/script_request.json")

	dsunit.PrepareDatastoreFor(t, "mytestdb", "test://test/", "Read")
	services, err := getServices()
	if err != nil {
		t.Errorf("Failed to get services %v", err)
	}

	for _, service := range services {
		{
			response := service.GetByID(1)
			assert.Equal(t, "ok", response.Status, response.Message)
			assert.NotNil(t, response)
			assert.NotNil(t, response.Result)
			assert.Equal(t, "Abc", response.Result.Name)
			assert.Equal(t, true, *response.Result.Status)
		}

		{
			response := service.GetByIDs(1, 3)
			assert.NotNil(t, response)
			assert.Equal(t, "ok", response.Status, response.Message)
			assert.Equal(t, 2, len(response.Result))
			assert.Equal(t, "Abc", response.Result[0].Name)
			assert.Equal(t, "Cde", response.Result[1].Name)
		}

	}
}

func TestPersist(t *testing.T) {
	if dsunit.SkipTestIfNeeded(t) {
		return
	}
	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")

	services, err := getServices()
	if err != nil {
		t.Errorf("Failed to get services %v", err)
	}

	for _, service := range services {
		{
			falseValue := false
			dsunit.ExecuteScriptFromURL(t, "test://test/script_request.json")
			dsunit.PrepareDatastoreFor(t, "mytestdb", "test://test/", "Persist")
			response := service.GetByID(1)
			assert.Equal(t, "ok", response.Status, response.Message)

			interest := response.Result
			interest.Category = "Alphabet"

			var interests = make([]examples.Interest, 0)
			interests = append(interests, *interest)
			interests = append(interests, examples.Interest{Name: "Klm", Category: "Ubf", Status: &falseValue, GroupName: "AAAA"})
			persistResponse := service.Persist(interests)
			assert.NotNil(t, persistResponse)
			assert.Equal(t, "ok", persistResponse.Status, persistResponse.Message)

			assert.NotNil(t, persistResponse.Result)
			assert.Equal(t, 2, len(persistResponse.Result))
			dsunit.ExpectDatasetFor(t, "mytestdb", dsunit.FullTableDatasetCheckPolicy, "test://test/", "Persist")
		}
	}

}

func TestDelete(t *testing.T) {
	if dsunit.SkipTestIfNeeded(t) {
		return
	}
	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	services, err := getServices()
	if err != nil {
		t.Errorf("Failed to get services %v", err)
	}
	for _, service := range services {
		{
			dsunit.ExecuteScriptFromURL(t, "test://test/script_request.json")
			dsunit.PrepareDatastoreFor(t, "mytestdb", "test://test/", "Delete")

			deleteResponse := service.DeleteByID(1)
			assert.NotNil(t, deleteResponse)
			assert.Equal(t, "ok", deleteResponse.Status, deleteResponse.Message)

			dsunit.ExpectDatasetFor(t, "mytestdb", dsunit.FullTableDatasetCheckPolicy, "test://test/", "Delete")
		}
	}
}
