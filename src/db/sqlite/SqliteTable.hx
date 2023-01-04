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

    public function new(db:IDatabase) {
        this.db = db;
    }

    public function all():Promise<DatabaseResult<Array<Record>>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'all'));
                return;
            }
            nativeDB.all(buildSelect(this)).then(response -> {
                var records = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                resolve(new DatabaseResult(db, this, records));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "connect"));
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
            var sql = buildInsert(this, record, values);
            nativeDB.get(sql, values).then(response -> {
                resolve(new DatabaseResult(db, this, record));
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
            var values = [];
            var sql = buildSelect(this, query, values);
            nativeDB.all(sql, values).then(response -> {
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
            nativeDB.get(buildSelect(this, query, 1)).then(response -> {
                resolve(new DatabaseResult(db, this, Record.fromDynamic(response.data)));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "connect"));
            });
        });
    }

    private var nativeDB(get, null):NativeDatabase;
    private function get_nativeDB():NativeDatabase {
        return @:privateAccess cast(db, SqliteDatabase)._db;
    }
}