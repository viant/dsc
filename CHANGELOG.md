## March 17 2020 0.16.1
- Update sql dialect with session control

## Feb 13 2020 0.16.0
- Added ColumnTypes to Scanner interface

## September 17 2019 0.14.1
- Added vertica dialect
- Added dialect IsKeyCheckSessionLevel

## August 5 2019 0.12.0
 - Added CopyLocalInsert batch support

## MAy 8 2019 0.10.0
 - Added config.Clone
 
## MAy 7 2019 0.9.0
 - Changed IsNullable as interface type
 - Added dialect.BulkInsertType
 - Added INSERT ALL support
 - Patched oracle show create table
 - Patched CreateDatastore ora dialect
 - Patched  duplicate column DML builder

## April 28 2019 0.8.2
 - Patched nil pointer on keySetter

## April 15 2019 0.8.0
 - Secured raw description credentials
    * Added DsnDescription method
    * Removed SecuredDescriptor

## April 15 2019 0.7.1
    * Patched batch insert

## April 15 2019 0.7.0
    * Added dynamic sql driver
    * Added request limiter

## April 15 2019 0.6.5
    * Reduced connection time 
    
## April 5 2019 0.6.4
    * Patched connection leackage
    
## March 12 2019 0.6.0
   * Added Dialect.Ping to check/wait if database if online 

## Feb 23 2019 0.5.0
   * Added persist support with toolbox.Ranger, toolbox.Iterator data types 

## Feb 6 2019 0.4.4
    * Patched getColumn with metadata

## Feb 6 2019 0.4.3
    * Patched show create table dialect embedding issue

## Jan 31 2018 0.4.2
    * Patched persisting row with nil primary key value

## Jan 19 2018 0.4.1
    * Change CreateTable specificiation data type
    * Update sql parser with mili column IN clause

## Dec 27 2018 0.3.0
    * Added casandra dialect
    * Change Persist to allow table without pk (for insert only operation)
    * Added CanHandleTransaction to dialect

## Nov 5 2018 0.2.0
    * Extended dialect with ShowCreateTable

## Jul 1 2016 (Alpha)

  * Initial Release.
