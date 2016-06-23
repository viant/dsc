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

//InterestService a test service
type InterestService interface {

	//GetById returns interest by id
	GetByID(id  int) *GetByIDResponse

	//GetByIds returns interests by passed in ids
	GetByIDs(id  ...int) *GetByIDsResponse

	//Persist persists passed in interests
	Persist(interests []Interest) *PersistResponse

	//DeleteById deletes interestes by id.
	DeleteByID(id int) *Response

}

//Response represents a response.
type Response struct {
	Status string
	Message string
}

//GetByIDResponse represents get by id response.
type GetByIDResponse struct {
	Response
	Result *Interest
}

//GetByIDsResponse represents a get by ids response.
type GetByIDsResponse struct {
	Response
	Result []Interest
}

//PersistResponse represents a persist response.
type PersistResponse struct {
	Response
	Result []Interest
}

