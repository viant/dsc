# Introduction

This package describes the datastore connectivity (dsc) API in detail.



- [Read operation](#Read-operation)
- [Persist operation](#Persist-operation)
- [Delete operation](#Delete-operation)
- [Execute SQL](#Execute-SQL)
- [API Reference](#API-Reference)


## Getting instance of Manager

Each datastore may use different configuration parameters, so see for specific vendor/plugin details
The best was of getting manager is by using ManagerFactory.
 
 
```go

    factory := dsc.NewManagerFactory()
    config := dsc.NewConfig("mysql", "[user]:[password]@[url]", "user:myuser,password:dev,url:tcp(127.0.0.1:3306)/mydbname?parseTime=true")
    manager, err := factory.Create(config)
    if err != nil {
            panic(err.Error())
    }
    
    ...
    manager := factory.CreateFromURL("file:///etc/myapp/datastore.json")
    
```


/etc/myapp/datastore.json
```json
  {
   "DriverName": "bigquery",
   "Parameters": {
            "serviceAccountId": "****@developer.gserviceaccount.com",
            "privateKeyPath": "/etc/test_service.pem",
            "projectId": "spheric-arcadia-98015",
            "datasetId": "MyDataset",
            "dateFormat": "yyyy-MM-dd HH:mm:ss z"
        }
   }

```


Connection configuration examples:




|Datastore | driver name | Descriptor | Example of encoded parameters | Native driver |
|---|---|---|---|---|
|MySql|mysql|[user]:[password]@[url]|**user**:myuser,**password**:dev,**url**:tcp(127.0.0.1:3306)/mydbname?parseTime=true|github.com/go-sql-driver/mysql|
|Postgres|pg | postgres://[user]:[password]@[url] |**user**:myuser,**password**:dev,**url**:localhost/mydbname?sslmode=verify-full|github.com/lib/pq| 
|MS SQL Server|mssql|server=[server];user id=[user];password:[password];database:[database]|**user**:myuser,**password**:dev,**sevrver**:localhost,**database**:mydbname|github.com/denisenkom/go-mssqldb|
|Oracle|ora|[user]/[password]@[url]|**user**:myuser,**password**:dev,**url**:localhost:1521/mysid|github.com/rana/ora|
|BigQuery|bigquery|n/a|**serviceAccountId**:myseriveAccount,**privateKeyPath**:/somepath/key.pem,**projectId**:myproject,**datasetId**:MyDataset,**dateFormat**:yyyy-MM-dd hh:mm:ss z|github.com/viant/bgc|
|service|aerospike|n/a|**host**:127.0.0.1,**port**:3000,**namespace**:test,**dateFormat**:yyyy-MM-dd hh:mm:ss|github.com/viant/asc|
|New line delimiter JSON|ndjson|n/a|**url**:someUrl,**ext**:.json,**dateFormat**:yyyy-MM-dd hh:mm:ss|github.com/viant/dsc|


Note that sql drivers use driver name and descriptor as sql.Open(driver, descriptor)



## Tags meta mapping

This library allows custom reading and persisting records, but also automatic field mapping
In this scenario if column tag is not defined the filed name will be used.


|Tag name | Description |
|---|---|
|column | Column name of table in datastore |
|primaryKey| Flag indicating if column is part of primary key|
|autoincrement| Flag indicating if column uses autoincrement |
|sequence| Sequence name  (not implemented yey) |
|dateFormat | SimpleDateFormat date format format style |
|dateLayout | Golang date layout |
|transient | Ignores filed in datastore operations |
|valueMap | value mapping after fetching record, and before persisting data|


```go


type User struct {
	Id          int `autoincrement:"true"`
	Name        string `column:"name"`
	DateOfBirth time.Time `column:"dob" dateFormat:"yyyyy-MM-dd HH:mm:ss"`
	SessionId    string `transient:"true"`
}




```

<a name="Read-operation"></a>
## Read operation

High-level datastore read operation involves the following:
1 Opening connection
2 Opening cursor/statement with SQL specyfing data source 
3 Fetching data
  * Mapping each fetched data record into application domain class
4 Closing cursor/statement
5 Closing connection

This api has been design to hide the most of these operation.
Read operation just requires SELECT statement and optionally record mapper.

Behind the scene, for NoSQL datastore this library comes with basic Query parser to easily map structured Query into NoSQL fields.


### Reading with with default record mapper

It is possible to use default MetaRecordMapper that uses tags defined in application model class. The following tags are supported
1 column - name of datastore field/column
2 autoincrement - boolean flag to use autoincrement, in this case on insert the value can be automatically set back on application model class
3 primaryKey - boolean flag primary key
4 dateLayout - date layout check string to time.Time conversion
4 dateFormat - date format check java simple date format
5 sequence - name of sequence used to generate next id
6 transient - boolean flag to not map a field with record data 
7 valueMap - value maping that will be applied after fetching a record and before writing it to datastore. 
    For instance valueMap:"yes:true,no:false" would map yes to true, no to false 



```go

    type User struct {
        Id int `autoincrement:"true"`
        Username string                 //if column and field are same - no mapping needed 
        LastAccessTime *time.Time  `column:"last_access_time" dateLayout:"2006-01-02 15:04:05"`
        CachedField bool `transient:"true"`
    }

    //reading single record
	user := User{}
	success, err:= manager.ReadSingle(&user, "SELECT id, username FROM users WHERE id = ?", []interface{}{1}, nil)


    //reading all records
    var users=make([]User, 0)
    err:= manager.ReadAll(&users, "SELECT id, username FROM users", nil, nil)


```


### Reading with custom record mapper

In this scenario RecordMapper is responsible for mapping datatstore data record into application model class.

```go

    type UserRecordMapper struct {}
    
    func (this *UserRecordMapper) Map(scanner  dsc.Scanner) (interface{}, error) {
        user := User{}
        var name = ""
       scanner.Scan(
            &user.Id,
            &name
        )
        user.Username := name
        
        return &user, nil
    }


    //reading single record
	user := User{}
	var recordMapper dsc.RecordMapper = &UserRecordMapper{}
	success, err:= manager.ReadSingle(&user, "SELECT id, username FROM users WHERE id = ?", []interface{}{1}, recordMapper)
    ...
    
    //reading all records
    var users=make([]User, 0)
    var recordMapper dsc.RecordMapper = &UserRecordMapper{}
    err:= manager.ReadAll(&users, "SELECT id, username FROM users", nil, recordMapper)


```

### Reading with custom reading handler

In this scenario custom reading handler is responsible for mapping data record in the application domain class.
The reading handler returns bool flag to instruct reader to continue reading more records.  

```go

    // single record reading
    user := &User{}
	err:= manager.ReadAllWithHandler("SELECT id, username FROM users WHERE id = ?", []interface{}{1}, func(scanner  dsc.Scanner) (toContinue bool, err error) {
		err =scanner.Scan(&user.Id, &user.Username)
		if err != nil {
		    return fale, err
		}
		return false, nil //since handler only needs one record it returns false (toContinue)
	})
	//...

    // all records reading
	var users = make([]User, 0)
	err:= manager.ReadAllWithHandler("SELECT id, username FROM users WHERE username LIKE ?", []interface{}{"Adam"}, func(scanner  dsc.Scanner) (toContinue bool, err error) {
        user := &User{}
        err =scanner.Scan(&user.Id, &user.Username)
        if err != nil {
            return fale, err
        }
        users := append(users, user)
        return true, nil // since handlers needs all recors it returns true
    })
    //...

```


<a name="Persist-operation"></a>
## Persist operation

High-level persisting operation involves the following:
1 Opening connection and transaction if possible
2 Identifying which items needs to be inserted or updated if possible
3 Inserting all insertable items, retrieve autoincrement/sequence value if available
4 Updating all updatable items
5 Issuing commit or rollback transaction if possible
6 Closing connection


This api has been design to hide the most of these operation.
Persist operation just requires data, target table and optionally DML provider, which needs to provide  parametrized sql for all INSERT, UPDATE and DELETE operations.

Behind the scene, for NoSQL datastore this library comes with basic DML parser to easily map structured DML statement into NoSQL similar operation.

## Persisting with default DmlProvider

Similarly like with  default MetaRecordMapper, it is possible to use tags definition on application model class to automate all operations required by DmlProvider.
MetaDmlProvider is an implementation that uses meta tags on application model class.
 
```go
 
    type User struct {
        Id int
        Username string
    }
    
    
    user :=	User{Id:1, Username:"Sir Edi"}
    inserted, updated, err:= manager.PersistSingle(&user, "users", nil)
    
    
    
    users := []User {
 		User{
 			Id:1,
 			Username:"Sir Edi",
 		},
 		User{
 			Username:"Bogi",
 		},
 	}
 	inserted, updated, err:= manager.PersistAll(&users, "users", nil)
```
	

## Persisting with custom DmlProvider

In this scenario DmlProvider implementation is responsible for providing parametrized sql,  primary key columns, and values, updating autoincrement fields if needed.

```go

    type User struct {
        Id int
        Username string
    }
    
    type UserDmlProvider struct {}

    func (this UserDmlProvider) Get(operationType int, instance interface{}) *dsc.ParametrizedSQL {
        user:=instance.(User)
        switch operationType {
        case dsc.SqlTypeInsert:
            return &dsc.ParametrizedSQL{
                Sql    :"INSERT INTO users(id, username) VALUES(?, ?)",
                Values: []interface{}{user.Id, user.Username},
    
            }
    
        case dsc.SqlTypeUpdate:
            return &dsc.ParametrizedSQL{
                Sql    :"UPDATE users SET username = ? WHERE id = ?",
                Values: []interface{}{user.Id, user.Username},
    
            }
    
        }
        panic(fmt.Sprintf("Unsupported sql type:%v", operationType))
    }

    
    func (this UserDmlProvider) SetKey(instance interface{}, seq int64) {
        user:=instance.(*User)
        user.Id = int(seq)
    }
    
    
    func (this UserDmlProvider) Key(instance interface{}) [] interface{} {
        user:=instance.(User)
        return []interface{}{user.Id}
    }
    
    
    func (this UserDmlProvider) PkColumns() []string {
        return []string{"id"}
    }
    
    func NewUserDmlProvider() dsc.DmlProvider {
        var dmlProvider dsc.DmlProvider = &UserDmlProvider{}
        return dmlProvider
    }


    user :=	User{Id:1, Username:"Sir Edi"}
	inserted, updated, err:= manager.PersistSingle(&user, "users", NewUserDmlProvider())

    users := []User {
		User{
			Id:1,
			Username:"Sir Edi",
		},
		User{
			Username:"Bogi",
		},
	}
	inserted, updated, err:= manager.PersistAll(&users, "users", NewUserDmlProvider())
	

```


<a name="Delete operation"></a>
## Delete operation

 
### Deleting with default KeyProvider


```go
 
    type User struct {
        Id int
        Username string
    }
    
    
    user :=	User{Id:1, Username:"Sir Edi"}
    success, err:= manager.DeleteSingle(&user, "users", nil)
    
    
    users := []User {
 		User{
 			Id:1,
 		},
 		User{
 			Id:2,
 		},
 	}
 	deleted, err:= manager.DeleteAll(&users, "users", nil)
```
	
<a name="Execute-SQL"></a>
## Execut SQL commands

To execute any command supported by given datastore Execute method has been provided.

<a name="API-Reference"></a>
## API Reference

- [API Interfaces](./../api.go)

