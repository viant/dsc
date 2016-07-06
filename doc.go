/*
Package dsc - datastore connectivity library
This library provides connection capabilities to SQL, noSQL datastores or structured files providing sql layer on top of it.

For native database/sql it is just a ("database/sql") proxy, and for noSQL it supports simple SQL that is being translated to
put/get,scan,batch native NoSQL operations.


Datastore Manager implements read, persist (no insert nor update), and delete operations.
Read operation requires data record mapper,
Persist operation requires dml provider.
Delete operation requries key provider.

Datastore Manager provides default record mapper and dml/key provider for a struct, if no actual implementation is passed in.

The following tags can be used

1 column - name of datastore field/column

2 autoincrement - boolean flag to use autoincrement, in this case on insert the value can be automatically set back on application model class

3 primaryKey - boolean flag primary key

4 dateLayout - date layout check string to time.Time conversion

4 dateFormat - date format check java simple date format

5 sequence - name of sequence used to generate next id

6 transient - boolean flag to not map a field with record data

7 valueMap - value maping that will be applied after fetching a record and before writing it to datastore.
    For instance valueMap:"yes:true,no:false" would map yes to true, no to false



Usage:

	type Interest struct {
		Id int	`autoincrement:"true"`
		Name string
		ExpiryTimeInSecond int `column:"expiry"`
		Category string
	}

 	manager := factory.CreateFromURL("file:///etc/mystore-config.json")
        interest := Interest{}

    	intersts = make([]Interest, 0)
        err:= manager.ReadAll(&interests, SELECT id, name, expiry, category FROM interests", nil ,nil)
    	if err != nil {
        	panic(err.Error())
    	}
    	...

	inserted, updated, err:= manager.PersistAll(&intersts, "interests", nil)
	if err != nil {
        	panic(err.Error())
   	}

*/
package dsc
