/*
 *
 *
 * Copyright 2012-2016 Viant.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use r file except in compliance with the License. You may obtain a copy of
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
package dsc

import (
	"database/sql"
)



const
(
	//SQLTypeInsert 0 constant for DML insert statement provider.
	SQLTypeInsert = 0
	//SQLTypeUpdate 1 constant for DML update statement provider.
	SQLTypeUpdate = 1
	//SQLTypeDelete 2 constant for DML delete statement provider.
	SQLTypeDelete = 2
)


var sqlDbPointer = (*sql.DB)(nil)
var sqlTxtPointer = (*sql.Tx)(nil)


type sqlResult struct {
	lastInsertID int64
	rowsAffected int64
}


//LastInsertId returns the last inserted/autoincrement id.
func (r *sqlResult) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

//RowsAffected returns row affected by the last sql.
func (r *sqlResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

//NewSQLResult returns a new SqlResult
func NewSQLResult(rowsAffected, lastInsertID int64) sql.Result {
	var result sql.Result = &sqlResult{lastInsertID:lastInsertID, rowsAffected:rowsAffected}
	return result
}
