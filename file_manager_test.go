package dsc_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
)

type MostLikedCity struct {
	City      string
	Visits    int
	Souvenirs []string
}

type Traveler struct {
	Id            int
	Name          string
	LastVisitTime time.Time
	Achievements  []string
	MostLikedCity MostLikedCity
	VisitedCities []struct {
		City   string
		Visits int
	}
}

func TestInsert(t *testing.T) {
	config := dsc.NewConfig("ndjson", "[url]", "dateFormat:yyyy-MM-dd hh:mm:ss,ext:json,url:"+dsunit.ExpandTestProtocolAsURLIfNeeded("test:///test/"))
	manager, err := dsc.NewManagerFactory().Create(config)
	assert.Nil(t, err)
	for i := 0; i < 10; i++ {
		connection, err := manager.ConnectionProvider().Get()
		assert.Nil(t, err)
		defer connection.Close()
		assert.NotNil(t, connection)
	}

}

func TestPersist(t *testing.T) {
	config := dsc.NewConfig("ndjson", "[url]", "dateFormat:yyyy-MM-dd hh:mm:ss,ext:json,url:"+dsunit.ExpandTestProtocolAsURLIfNeeded("test:///test/"))
	manager, err := dsc.NewManagerFactory().Create(config)
	assert.Nil(t, err)
	dialect := dsc.GetDatastoreDialect("ndjson")
	datastore, err := dialect.GetCurrentDatastore(manager)
	assert.Nil(t, err)
	err = dialect.DropTable(manager, datastore, "travelers2")
	assert.Nil(t, err)

	travelers := make([]*Traveler, 2)
	travelers[0] = &Traveler{
		Id:            10,
		Name:          "Cook",
		LastVisitTime: time.Now(),
		Achievements:  []string{"abc", "jhi"},
		MostLikedCity: MostLikedCity{City: "Cracow", Visits: 4},
	}

	travelers[1] = &Traveler{
		Id:            20,
		Name:          "Robin",
		LastVisitTime: time.Now(),
		Achievements:  []string{"w", "a"},
		MostLikedCity: MostLikedCity{"Moscow", 3, []string{"s3", "sN"}},
	}

	inserted, updated, err := manager.PersistAll(&travelers, "travelers2", nil)
	assert.Nil(t, err)
	assert.Equal(t, 2, inserted)
	assert.Equal(t, 0, updated)

	travelers[1].Achievements = []string{"z", "g"}
	inserted, updated, err = manager.PersistSingle(travelers[1], "travelers2", nil)
	assert.Nil(t, err)
	assert.Equal(t, 0, inserted)
	assert.Equal(t, 1, updated)

	success, err := manager.DeleteSingle(travelers[0], "travelers2", nil)
	assert.Nil(t, err)
	assert.True(t, success)

}

func TestRead(t *testing.T) {
	config := dsc.NewConfig("ndjson", "[url]", "dateFormat:yyyy-MM-dd hh:mm:ss,ext:json,url:"+dsunit.ExpandTestProtocolAsURLIfNeeded("test:///test/"))
	manager, err := dsc.NewManagerFactory().Create(config)
	assert.Nil(t, err)

	{
		travelers := make([][]interface{}, 0)
		err := manager.ReadAll(&travelers, " SELECT id, name, lastVisitTime, visitedCities, achievements, mostLikedCity, LastVisitTime FROM travelers1 WHERE id IN(?)", []interface{}{1}, nil)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(travelers))
		assert.EqualValues(t, 1, travelers[0][0])
		assert.EqualValues(t, "Rob", travelers[0][1])
	}

	//
	//{
	//	var travelers = make([]Traveler, 0)
	//	err = manager.ReadAll(&travelers, " SELECT id, name, lastVisitTime, visitedCities, achievements, mostLikedCity, LastVisitTime FROM travelers1 WHERE id IN(?, ?)", []interface{}{1, 4}, nil)
	//	assert.Nil(t, err)
	//	assert.Equal(t, 2, len(travelers))
	//
	//	{
	//		traveler := travelers[0]
	//		assert.Equal(t, 1, traveler.Id)
	//		assert.Equal(t, "Rob", traveler.Name)
	//		assert.Equal(t, 2, len(traveler.VisitedCities))
	//		assert.Equal(t, 3, traveler.VisitedCities[0].Visits)
	//		assert.Equal(t, "Warsaw", traveler.VisitedCities[0].City)
	//		assert.Equal(t, 2, len(traveler.Achievements))
	//		assert.Equal(t, int64(1456801800), traveler.LastVisitTime.Unix())
	//	}
	//}
	//
	//{
	//	var travelers = make([]Traveler, 0)
	//	err = manager.ReadAll(&travelers, " SELECT id, name, lastVisitTime, visitedCities, achievements, mostLikedCity, LastVisitTime FROM travelers1", nil, nil)
	//	assert.Nil(t, err)
	//	assert.Equal(t, 4, len(travelers))
	//
	//	{
	//		traveler := travelers[0]
	//		assert.Equal(t, 1, traveler.Id)
	//		assert.Equal(t, "Rob", traveler.Name)
	//		assert.Equal(t, 2, len(traveler.VisitedCities))
	//		assert.Equal(t, 3, traveler.VisitedCities[0].Visits)
	//		assert.Equal(t, "Warsaw", traveler.VisitedCities[0].City)
	//		assert.Equal(t, 2, len(traveler.Achievements))
	//		assert.Equal(t, int64(1456801800), traveler.LastVisitTime.Unix())
	//	}
	//}
	//
	//{
	//	traveler := Traveler{}
	//	success, err := manager.ReadSingle(&traveler, " SELECT id, name, lastVisitTime, visitedCities, achievements, mostLikedCity, LastVisitTime FROM travelers1 WHERE id = ?", []interface{}{1}, nil)
	//	assert.Nil(t, err)
	//	assert.True(t, success)
	//}
	//
	//{
	//	traveler := Traveler{}
	//	success, err := manager.ReadSingle(&traveler, " SELECT id, name, lastVisitTime, visitedCities, achievements, mostLikedCity, LastVisitTime FROM travelers1 WHERE id IN(?)", []interface{}{1}, nil)
	//	assert.Nil(t, err)
	//	assert.True(t, success)
	//}
	//
	//
	//{
	//	traveler := make([]interface{}, 0)
	//	success, err := manager.ReadSingle(&traveler, " SELECT id, name, lastVisitTime, visitedCities, achievements, mostLikedCity, LastVisitTime FROM travelers1 WHERE id IN(?)", []interface{}{1}, nil)
	//	assert.Nil(t, err)
	//	assert.True(t, success)
	//}
	//
	//{
	//	traveler := make([]interface{}, 0)
	//	success, err := manager.ReadSingle(&traveler, " SELECT id, name, lastVisitTime, visitedCities, achievements, mostLikedCity, LastVisitTime FROM travelers1 WHERE id IN(?)", []interface{}{1}, nil)
	//	assert.Nil(t, err)
	//	assert.True(t, success)
	//}

}
