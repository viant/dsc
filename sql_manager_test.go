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
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
)

func Init(t *testing.T) dsc.Manager {
	config := dsc.NewConfig("mysql", "[user]:[password]@[url]", "user:root,password:dev,url:tcp(127.0.0.1:3306)/bar?parseTime=true")
	factory := dsc.NewManagerFactory()
	manager, _ := factory.Create(config)
	sqls := []string{
		"DROP TABLE  IF EXISTS users",
		"CREATE TABLE `users` (`id` int(11) NOT NULL AUTO_INCREMENT,`username` varchar(255) DEFAULT NULL,`active` tinyint(1) DEFAULT '1',`salary` decimal(7,2) DEFAULT NULL,`comments` text,`last_access_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,PRIMARY KEY (`id`)) ENGINE=InnoDB",
		"INSERT INTO users(username, active, salary, comments, last_access_time) VALUES('Edi', 1, 43000, 'no comments', NOW());",
	}
	for _, sql := range sqls {
		_, err := manager.Execute(sql)
		if err != nil {
			t.Fatalf("Failed to init database %v", err)
		}
	}
	return manager
}

type User struct {
	Id             int `autoincrement:"true"`
	Username       string
	Active         bool
	LastAccessTime *time.Time `column:"last_access_time" dateFormat:"2006-01-02 15:04:05"`
	Salary         float64    `column:"salary"`
	Comments       string
}

func (this User) String() string {
	return fmt.Sprintf("Id: %v, Name: %v, Active:%v, Salary: %v Comments: %v, Last Access Time %v\n", this.Id, this.Username, this.Active, this.Salary, this.Comments, this.LastAccessTime)
}

type UserRecordMapper struct{}

func (this *UserRecordMapper) Map(scanner dsc.Scanner) (interface{}, error) {
	user := User{}
	err := scanner.Scan(
		&user.Id,
		&user.Username,
		&user.Active,
		&user.Salary,
		&user.Comments,
		&user.LastAccessTime,
	)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func TestReadSingleWithCustomHandler(t *testing.T) {
	manager := Init(t)
	user := &User{}

	err := manager.ReadAllWithHandler("SELECT id, username FROM users WHERE id = ?", []interface{}{1}, func(scanner dsc.Scanner) (bool, error) {
		err := scanner.Scan(&user.Id, &user.Username)
		if err != nil {
			t.Errorf("Error %v", err)
		}
		return false, nil
	})

	if err != nil {
		t.Error("Failed test: " + err.Error())
	}
	assert.Equal(t, "Edi", user.Username)

}
func TestReadSingleWithCustomMapperDataset(t *testing.T) {
	manager := Init(t)
	singleUser := User{}
	var recordMapper dsc.RecordMapper = &UserRecordMapper{}
	success, err := manager.ReadSingle(&singleUser, "SELECT id, username, active, salary, comments,last_access_time FROM users WHERE id = ?", []interface{}{1}, recordMapper)
	if err != nil {
		t.Error("Failed test: " + err.Error())
	}
	assert.Equal(t, true, success, "Should fetch a user")
	assert.Equal(t, "Edi", singleUser.Username)

}

func TestReadAllWithCustomMapperDataset(t *testing.T) {
	manager := Init(t)
	var users = make([]User, 0)
	var recordMapper dsc.RecordMapper = &UserRecordMapper{}
	err := manager.ReadAll(&users, "SELECT id, username, active, salary, comments,last_access_time FROM users WHERE id = ?", []interface{}{1}, recordMapper)
	if err != nil {
		t.Error("Failed test: " + err.Error())
	}
	assert.Equal(t, 1, len(users))
	user := users[0]
	assert.Equal(t, "Edi", user.Username)

}

func TestReadSingleWithDefaultMetaMapper(t *testing.T) {
	manager := Init(t)
	singleUser := User{}

	success, err := manager.ReadSingle(&singleUser, "SELECT id, username, active, salary, comments,last_access_time FROM users WHERE id = ?", []interface{}{1}, nil)
	if err != nil {
		t.Error("Failed test: " + err.Error())
	}
	assert.Equal(t, true, success, "Should fetch a user")
	assert.Equal(t, true, singleUser.Active)
}

func TestReadAllWithDefaultMetaMapper(t *testing.T) {
	manager := Init(t)
	var users = make([]User, 0)

	err := manager.ReadAll(&users, "SELECT id, username, active, salary, comments,last_access_time FROM users WHERE id = ?", []interface{}{1}, nil)
	if err != nil {
		t.Error("Failed test: " + err.Error())
	}
	assert.Equal(t, 1, len(users))
	user := users[0]
	assert.Equal(t, "Edi", user.Username)
}

func TestReadSingleRowAsSlice(t *testing.T) {
	manager := Init(t)
	var aUser = make([]interface{}, 0)

	success, err := manager.ReadSingle(&aUser, "SELECT id, username, active, salary, comments,last_access_time FROM users WHERE id = ?", []interface{}{1}, nil)
	if err != nil {
		t.Error("Failed test: " + err.Error())
	}
	assert.Equal(t, true, success, "Should fetch a user")
	assert.EqualValues(t, 1, aUser[0].(int64))
}

func TestReadSingleAllRowAsSlice(t *testing.T) {
	manager := Init(t)
	var users = make([][]interface{}, 0)

	err := manager.ReadAll(&users, "SELECT id, username, active, salary, comments,last_access_time FROM users WHERE id = ?", []interface{}{1}, nil)
	if err != nil {
		t.Error("Failed test: " + err.Error())
	}

	assert.Equal(t, 1, len(users))
	user := users[0]
	assert.Equal(t, "Edi", user[1].(string))
}

type UserDmlProvider struct{}

func (this UserDmlProvider) Get(operationType int, instance interface{}) *dsc.ParametrizedSQL {
	user := instance.(User)
	switch operationType {
	case dsc.SQLTypeInsert:
		return &dsc.ParametrizedSQL{
			SQL:    "INSERT INTO users(id, username) VALUES(?, ?)",
			Values: []interface{}{user.Id, user.Username},
		}

	case dsc.SQLTypeUpdate:
		return &dsc.ParametrizedSQL{
			SQL:    "UPDATE users SET username = ? WHERE id = ?",
			Values: []interface{}{user.Id, user.Username},
		}

	}
	panic(fmt.Sprintf("Unsupported sql type:%v", operationType))
}

func (this UserDmlProvider) SetKey(instance interface{}, seq int64) {
	user := instance.(*User)
	user.Id = int(seq)
}

func (this UserDmlProvider) Key(instance interface{}) []interface{} {
	user := instance.(User)
	return []interface{}{user.Id}
}

func (this UserDmlProvider) PkColumns() []string {
	return []string{"id"}
}

func NewUserDmlProvider() dsc.DmlProvider {
	var dmlProvider dsc.DmlProvider = &UserDmlProvider{}
	return dmlProvider
}

func TestPersistAllWithCustomDmlProvider(t *testing.T) {
	manager := Init(t)
	users := []User{
		User{
			Id:       1,
			Username: "Sir Edi",
			Active:   false,
			Salary:   32432.3,
		},
		User{
			Username: "Bogi",
			Active:   true,
			Salary:   32432.3,
		},
	}
	inserted, updated, err := manager.PersistAll(&users, "users", NewUserDmlProvider())
	if err != nil {
		t.Error("Failed test: " + err.Error())
	}
	assert.Equal(t, 1, inserted)
	assert.Equal(t, 0, updated)
}

func TestPersistAllWithDefaultDmlProvider(t *testing.T) {
	manager := Init(t)
	users := []User{
		User{
			Id:       1,
			Username: "Sir Edi",
			Active:   false,
			Salary:   32432.3,
		},
		User{
			Username: "Bogi",
			Active:   true,
			Salary:   32432.3,
		},
	}
	inserted, updated, err := manager.PersistAll(&users, "users", nil)
	if err != nil {
		t.Error("Failed test: " + err.Error())
	}
	assert.Equal(t, 2, users[1].Id, "autoicrement value should be set")
	assert.Equal(t, 1, inserted)
	assert.Equal(t, 1, updated)
	user := User{Username: "KLK", Active: false}
	inserted, updated, err = manager.PersistSingle(&user, "users", nil)
	if err != nil {
		t.Error("Failed test: " + err.Error())
	}
	assert.Equal(t, 1, inserted)
	assert.Equal(t, 3, user.Id, "autoicrement value should be set")
}
