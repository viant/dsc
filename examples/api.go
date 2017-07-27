package examples

//InterestService a test service
type InterestService interface {

	//GetById returns interest by id
	GetByID(id int) *GetByIDResponse

	//GetByIds returns interests by passed in ids
	GetByIDs(id ...int) *GetByIDsResponse

	//PersistTable persists passed in interests
	Persist(interests []*Interest) *PersistResponse

	//DeleteById deletes interestes by id.
	DeleteByID(id int) *Response
}

//Response represents a response.
type Response struct {
	Status  string
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
	Result []*Interest
}

//PersistResponse represents a persist response.
type PersistResponse struct {
	Response
	Result []*Interest
}
