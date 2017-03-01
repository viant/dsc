package examples

import (
	"fmt"

	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

type interestServiceImpl struct {
	manager dsc.Manager
}

func setErrorStatus(response *Response, err error) {
	response.Message = err.Error()
	response.Status = "error"
}

func (s *interestServiceImpl) GetByID(id int) *GetByIDResponse {
	response := &GetByIDResponse{Response: Response{Status: "ok"}}
	interest := &Interest{}
	success, err := s.manager.ReadSingle(interest, "SELECT id, name, category, status FROM interests WHERE id = ?", []interface{}{id}, nil)
	if err != nil {
		setErrorStatus(&response.Response, err)
		return response
	}

	if success {
		response.Result = interest
	}
	return response
}

func (s *interestServiceImpl) GetByIDs(ids ...int) *GetByIDsResponse {
	response := &GetByIDsResponse{Response: Response{Status: "ok"}}
	var result = make([]*Interest, 0)
	err := s.manager.ReadAll(&result, fmt.Sprintf("SELECT id, name, category, status FROM interests WHERE id IN(%v)", toolbox.JoinAsString(ids, ",")), nil, nil)
	if err != nil {
		setErrorStatus(&response.Response, err)
		return response
	}
	response.Result = result
	return response
}

func (s *interestServiceImpl) Persist(interests []*Interest) *PersistResponse {
	response := &PersistResponse{Response: Response{Status: "ok"}}
	inserted, updated, err := s.manager.PersistAll(&interests, "interests", nil)
	if err != nil {
		setErrorStatus(&response.Response, err)
		return response
	}
	response.Result = interests
	response.Message = fmt.Sprintf("inserted %v, updated %v", inserted, updated)
	return response

}

func (s *interestServiceImpl) DeleteByID(id int) *Response {
	response := &Response{Status: "ok"}
	_, err := s.manager.DeleteSingle(&Interest{ID: id}, "interests", nil)
	if err != nil {
		setErrorStatus(response, err)
		return response
	}
	return response
}

//NewInterestService creates a new interests service
func NewInterestService(configURL string) (InterestService, error) {

	manager, err := dsc.NewManagerFactory().CreateFromURL(configURL)

	if err != nil {
		return nil, err
	}
	var result InterestService = &interestServiceImpl{manager: manager}
	return result, nil
}
