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
package examples

import (
	"fmt"
	"log"
	"net/http"

	"github.com/viant/toolbox"
)

var version = "/v1/"
var interestURI = version + "interest/"

//StartServer starts interests web service
func StartServer(configFile string, port string) {

	service, err := NewInterestService(configFile)
	if err != nil {
		panic(fmt.Sprintf("Failed to create service due to %v", err))
	}

	interestRouter := toolbox.NewServiceRouter(
		toolbox.ServiceRouting{
			HTTPMethod: "GET",
			URI:        interestURI + "{id}",
			Handler:    service.GetByID,
			Parameters: []string{"id"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "GET",
			URI:        interestURI + "{ids}",
			Handler:    service.GetByIDs,
			Parameters: []string{"ids"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        interestURI,
			Handler:    service.Persist,
			Parameters: []string{"interests"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "DELETE",
			URI:        interestURI + "{id}",
			Handler:    service.DeleteByID,
			Parameters: []string{"id"},
		},
	)

	http.HandleFunc(interestURI, func(response http.ResponseWriter, request *http.Request) {

		errorHandler := func(message string) {
			response.WriteHeader(http.StatusInternalServerError)
			err := interestRouter.WriteResponse(toolbox.NewJSONEncoderFactory(), &Response{Status: "error", Message: message}, request, response)
			if err != nil {
				log.Fatal("Failed to write response :%v", err)
			}
		}
		defer func() {
			if err := recover(); err != nil {
				errorHandler(fmt.Sprintf("%v", err))
			}
		}()

		err := interestRouter.Route(response, request)
		if err != nil {
			errorHandler(fmt.Sprintf("%v", err))
		}
	})

	fmt.Printf("Started dsunit server on port %v\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
