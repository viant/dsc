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
package examples

import (
	"fmt"
	"github.com/viant/toolbox"
)


type interestServiceClient struct {
	url string
}



func setError(err error, response *Response) {
	response.Message = err.Error()
	response.Status = "error"
}



func (c *interestServiceClient) GetByID(id  int) *GetByIDResponse {
	response := &GetByIDResponse{}
	err := toolbox.RouteToService("get", fmt.Sprintf("%v%v",c.url, id), nil, response)
	if err != nil {
		setError(err, &response.Response)
	}
	return response
}

func (c *interestServiceClient) GetByIDs(ids  ...int) *GetByIDsResponse {
	response := &GetByIDsResponse{}
	err := toolbox.RouteToService("get", fmt.Sprintf("%v%v",c.url, toolbox.JoinAsString(ids, ",")), nil, response)
	if err != nil {
		setError(err, &response.Response)
	}
	return response
}

func (c *interestServiceClient) Persist(interests []Interest) *PersistResponse {
	response := &PersistResponse{}
	err := toolbox.RouteToService("post", fmt.Sprintf("%v",c.url, ), &interests, response)
	if err != nil {
		setError(err, &response.Response)
	}

	return response
}

func (c *interestServiceClient) DeleteByID(id int) *Response {
	response := &Response{}
	err := toolbox.RouteToService("delete", fmt.Sprintf("%v%v",c.url, id), nil, response)
	if err != nil {
		setError(err, response)
	}
	return response
}


//NewInterestServiceClient creates a new InterestService client
func NewInterestServiceClient(server string) InterestService {
	var result InterestService = &interestServiceClient{url:fmt.Sprintf("http://%v%v",server, interestURI)}
	return result
}