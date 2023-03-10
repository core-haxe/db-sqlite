package db.sqlite;

import promises.Promise;
import sqlite.SqliteError;
import sqlite.Database as NativeDatabase;
import db.sqlite.Utils.*;
import db.utils.SqlUtils.*;
import db.Query.QueryExpr;
import promises.PromiseUtils;

class SqliteTable implements ITable {
    public var db:IDatabase;
    public var name:String;
    public var exists:Bool;

    private var _relationships:Array<RelationshipDefinition> = null;

    public function new(db:IDatabase) {
        this.db = db;
    }

    private var _tableSchema:TableSchema = null;
    public function schema():Promise<DatabaseResult<TableSchema>> {
        return new Promise((resolve, reject) -> {
            if (_tableSchema != null) {
                resolve(new DatabaseResult(db, this, _tableSchema));
                return;
            }

            this.db.schema().then(result -> {
                _tableSchema = result.data.findTable(this.name);
                resolve(new DatabaseResult(db, this, _tableSchema));
            }, (error:DatabaseError) -> {
                reject(error);
            });
        });
    }

    public function all():Promise<DatabaseResult<Array<Record>>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'all'));
                return;
            }

            refreshSchema().then(schemaResult -> {
                var values = [];
                var sql = buildSelect(this, null, null, values, db.definedTableRelationships(), null, schemaResult.data);
                return nativeDB.all(sql, values);
            }).then(response -> {
                var records = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                resolve(new DatabaseResult(db, this, records));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "all"));
            });
        });
    }

    public function page(pageIndex:Int, pageSize:Int = 100, query:QueryExpr = null):Promise<DatabaseResult<Array<Record>>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'page'));
                return;
            }
            reject(new DatabaseError("not implemented", "page"));
        });
    }

    public function add(record:Record):Promise<DatabaseResult<Record>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'add'));
                return;
            }
            
            var values = [];
            var insertedId:Int = -1;
            var sql = buildInsert(this, record, values);
            var hasSequenceTable = false;
            var schema:DatabaseSchema = null;
            refreshSchema().then(result -> {
                schema = result.data;
                return nativeDB.get(sql, values);
            }).then(result -> {
                return nativeDB.get(SQL_TABLE_EXISTS, "sqlite_sequence");
            }).then(result -> {
                if (result.data != null) {
                    hasSequenceTable = true;
                }
                if (hasSequenceTable) {
                    return nativeDB.get(Utils.SQL_LAST_INSERTED_ID, this.name);
                }
                return null;
            }).then(result -> {
                if (result != null) {
                    insertedId = result.data.seq;
                    record.field("_insertedId", insertedId);
                    resolve(new DatabaseResult(db, this, record));
                } else {
                    resolve(new DatabaseResult(db, this, record));
                }
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "add"));
            });
        });
    }

    public function addAll(records:Array<Record>):Promise<DatabaseResult<Array<Record>>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'addAll'));
                return;
            }

            var promises = [];
            for (record in records) {
                promises.push(add.bind(record));
            }

            PromiseUtils.runSequentially(promises).then(results -> {
                resolve(new DatabaseResult(db, this, records));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "addAll"));
            });
        });
    }

    public function delete(record:Record):Promise<DatabaseResult<Record>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'delete'));
                return;
            }
            var values = [];
            var sql = buildDeleteRecord(this, record, values);
            nativeDB.get(sql, values).then(response -> {
                resolve(new DatabaseResult(db, this, record));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "delete"));
            });
        });
    }

    public function deleteAll(query:QueryExpr = null):Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'deleteAll'));
                return;
            }
            nativeDB.exec(buildDeleteWhere(this, query)).then(response -> {
                resolve(new DatabaseResult(db, this, true));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "deleteAll"));
            });
        });
    }

    public function update(query:QueryExpr, record:Record):Promise<DatabaseResult<Record>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'update'));
                return;
            }
            var values = [];
            var sql = buildUpdate(this, query, record, values);
            nativeDB.get(sql, values).then(response -> {
                resolve(new DatabaseResult(db, this, record));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "update"));
            });
        });
    }

    public function find(query:QueryExpr):Promise<DatabaseResult<Array<Record>>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'find'));
                return;
            }

            refreshSchema().then(schemaResult -> {
                var values = [];
                var sql = buildSelect(this, query, null, values, db.definedTableRelationships(), null, schemaResult.data);
                return nativeDB.all(sql, values);
            }).then(response -> {
                var records = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                resolve(new DatabaseResult(db, this, records));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "find"));
            });
        });
    }

    public function findOne(query:QueryExpr):Promise<DatabaseResult<Record>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'findOne'));
                return;
            }

            refreshSchema().then(schemaResult -> {
                var values = [];
                var sql = buildSelect(this, query, 1, values, db.definedTableRelationships(), null, schemaResult.data);
                return nativeDB.all(sql, values);
            }).then(response -> {
                resolve(new DatabaseResult(db, this, Record.fromDynamic(response.data[0])));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "findOne"));
            });
        });
    }

    private var nativeDB(get, null):NativeDatabase;
    private function get_nativeDB():NativeDatabase {
        return @:privateAccess cast(db, SqliteDatabase)._db;
    }

    private function refreshSchema():Promise<DatabaseResult<DatabaseSchema>> { // we'll only refresh the data schema if there are table relationships, since the queries might need them
        return new Promise((resolve, reject) -> {
            if (db.definedTableRelationships() == null) {
                resolve(new DatabaseResult(db, this, null));
                return;
            }

            db.schema().then(result -> {
                resolve(result);
            }, (error) -> {
                reject(error);
            });
        });
    }
}