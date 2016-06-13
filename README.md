# Datastore Connectivity (dsc)

[![Datastore Connectivity library for Go.](https://goreportcard.com/badge/github.com/viant/dsc)](https://goreportcard.com/report/github.com/viant/dsc)


This library is compatible with Go 1.5+

Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Motivation](#Motivation)
- [Usage](#Usage)
- [Prerequisites](#Prerequisites)
- [Installation](#Installation)
- [API Documentaion](#API-Documentation)
- [Tests](#Tests)
- [Examples](#Examples)
- [License](#License)
- [Credits and Acknowledgements](#Credits-and-Acknowledgements)



## Motivation

This library was developed as part of dsunit (Datastore unit testibility library) to provide unified access to SQL, noSQL, 
or any other store that deals with structured data in SQL-ish way.


## Usage:


The following is a very simple example of CRUD operations with dsc

```go
package main

import (
	"github.com/viant/dsc"
	_ "github.com/go-sql-driver/mysql"
)


type Interest struct {
	Id int	`autoincrement:"true"`
	Name string
	ExpiryTimeInSecond int `column:"expiry"`
	Category string
}


func main() {


	config := dsc.NewConfig("mysql", "[user]:[password]@[url]", "user:root,password:dev,url:tcp(127.0.0.1:3306)/mydb?parseTime=true")
	factory := NewManagerFactory()
	manager, err := factory.Create(config)
    if err != nil {
        panic(err.Error())
	}

    // manager := factory.CreateFromURL("file:///etc/myapp/datastore.json")
  
  
    interest := Interest{}
    
    success, err:= manager.ReadSingle(&interest, SELECT id, name, expiry, category FROM interests WHERE id = ?", []interface{}{id},nil)
	if err != nil {
        panic(err.Error())
	}

    var intersts = make([]Interest, 0)
    err:= manager.ReadAll(&interests, SELECT id, name, expiry, category FROM interests", nil ,nil)
    if err != nil {
        panic(err.Error())
    }

    
    intersts := []Interest {
        Interest{Name:"Abc", ExpiryTimeInSecond:3600, Category:"xyz"},
        Interest{Name:"Def", ExpiryTimeInSecond:3600, Category:"xyz"},
        Interest{Id:20, Name:"Ghi", ExpiryTimeInSecond:3600, Category:"xyz"},
    }


	inserted, updated, err:= manager.PersistAll(&intersts, "interests", nil)
	if err != nil {
        panic(err.Error())
   	}
   	fmt.Printf("Inserted %v, updated: %v\n", inserted, updated)
  
    deleted, err := manager.DeleteAll(&intersts, "intersts", nil)
    if err != nil {
        panic(err.Error())
   	}
 	fmt.Printf("Inserted %v, updated: %v\n", deleted)
  
}
```

More examples illustrating the use of the API are located in the
[`examples`](examples) directory.

Details about the API are available in the [`docs`](docs) directory.

<a name="Prerequisites"></a>
## Prerequisites

[Go](http://golang.org) version v1.5+ is required.

To install the latest stable version of Go, visit
[http://golang.org/dl/](http://golang.org/dl/)


Target


<a name="Installation"></a>
## Installation:

1. Install Go 1.5+ and setup your environment as [Documented](http://golang.org/doc/code.html#GOPATH) here.
2. Get the client in your ```GOPATH``` : ```go get github.com/viant/dsc```
 * To update the client library: ```go get -u github.com/viant/dsc```


### Some Hints:

 * To run a go program directly: ```go run <filename.go>```
 * to build:  ```go build -o <output> <filename.go>```



<a name="API-Documentation"></a>

## API Documentation

API documentation is available in the [`docs`](docs/README.md) directory.


## Tests

This library is packaged with a number of tests. Tests require Testify library.

Before running the tests, you need to update the dependencies:

    $ go get .

To run all the test cases with race detection:

    $ go test



<a name="Examples"></a>
## Examples

A simple CRUD applications is provided in the [`examples`](examples) directory.


<a name="License"></a>
## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.


<a name="Credits-and-Acknowledgements"></a>

##  Credits and Acknowledgements

**Library Author:** Adrian Witas
**Contributors:** Sudhakaran Dharmaraj