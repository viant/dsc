package examples_test

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/viant/dsc"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc/examples"
	"github.com/viant/dsunit"
	"fmt"
	"github.com/viant/toolbox/url"
)


//
//func init() {
//	go func() {
//		resource := url.NewResource("test/config/store.json")
//		examples.StartServer(resource.URL, "8084")
//	}()
//	time.Sleep(2 * time.Second)
//}


func getServices() ([]examples.InterestService, error) {


	resource := url.NewResource("test/config/store.json")
	local, err := examples.NewInterestService(resource.URL)
	if err != nil {
		return nil, err
	}

	return []examples.InterestService{local}, nil

	//client := examples.NewInterestServiceClient("127.0.0.1:8084")
	//return []examples.InterestService{local, client}, nil

}

func TestRead(t *testing.T) {
	dsunit.InitFromURL(t, "test/init.json")

	dsunit.PrepareDatastoreFor(t, "mytestdb", "test/", "Read")
	services, err := getServices()
	if err != nil {
		t.Errorf("failed to get services %v", err)
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


	services, err := getServices()
	if err != nil {
		t.Errorf("failed to get services %v", err)
	}

	for _, service := range services {
		{
			falseValue := false
			dsunit.InitFromURL(t, "test/init.json")
			dsunit.PrepareDatastoreFor(t, "mytestdb", "test/", "Persist")
			response := service.GetByID(1)
			assert.Equal(t, "ok", response.Status, response.Message)

			interest := response.Result
			interest.Category = "Alphabet"

			var interests = make([]*examples.Interest, 0)
			interests = append(interests, interest)
			interests = append(interests, &examples.Interest{Name: "Klm", Category: "Ubf", Status: &falseValue, GroupName: "AAAA"})
			persistResponse := service.Persist(interests)
			assert.NotNil(t, persistResponse)
			assert.Equal(t, "ok", persistResponse.Status, persistResponse.Message)

			assert.NotNil(t, persistResponse.Result)
			assert.Equal(t, 2, len(persistResponse.Result))
			dsunit.ExpectDatasetFor(t, "mytestdb", dsunit.FullTableDatasetCheckPolicy, "test/", "Persist")
		}
	}

}

func TestPersistAll(t *testing.T) {
	dsunit.InitFromURL(t, "test/init.json")

	services, err := getServices()
	if err != nil {
		t.Errorf("failed to get services %v", err)
	}

	service := services[0]
	var interests = make([]*examples.Interest, 0)
	for i := 1; i <= 1000; i++ {
		var status = true
		interests = append(interests, &examples.Interest{
			Name: fmt.Sprintf("Name %v", i),
			Category:"cat",
			Status  :&status,
			GroupName:"abc",
		})
	}
	startTime := time.Now().Unix()
	persistResponse := service.Persist(interests)
	assert.Equal(t, "ok", persistResponse.Status)
	endTime := time.Now().Unix()
	var elapsed  = endTime - startTime
	assert.True(t, elapsed < 60)//elapsed should 100k should be under 30 sec
}



func TestDelete(t *testing.T) {
	services, err := getServices()
	if err != nil {
		t.Errorf("failed to get services %v", err)
	}
	for _, service := range services {
		{
			dsunit.InitFromURL(t, "test/init.json")
			dsunit.PrepareDatastoreFor(t, "mytestdb", "test/", "Delete")

			deleteResponse := service.DeleteByID(1)
			assert.NotNil(t, deleteResponse)
			assert.Equal(t, "ok", deleteResponse.Status, deleteResponse.Message)

			dsunit.ExpectDatasetFor(t, "mytestdb", dsunit.FullTableDatasetCheckPolicy, "test/", "Delete")
		}
	}
}
