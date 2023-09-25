package db.sqlite;

import Query.QueryExpr;
import db.sqlite.Utils.*;
import db.utils.SqlUtils.*;
import promises.Promise;
import promises.PromiseUtils;
import sqlite.Database as NativeDatabase;
import sqlite.SqliteError;

using StringTools;

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

    public function clearCachedSchema() {
        _tableSchema = null;
    }

    public function applySchema(newSchema:TableSchema):Promise<DatabaseResult<TableSchema>> {
        return new Promise((resolve, reject) -> {
            
            schema().then(result -> {
                var promises = [];
                var currentSchema = result.data;
                if (currentSchema != null && !currentSchema.equals(newSchema)) {
                    var diff = currentSchema.diff(newSchema);

                    for (added in diff.addedColumns) {
                        promises.push(addColumn.bind(added));
                    }

                    for (removed in diff.removedColumns) {
                        promises.push(removeColumn.bind(removed));
                    }
                }
                return PromiseUtils.runSequentially(promises);
            }).then(result -> {
                clearCachedSchema();
                cast(db, SqliteDatabase).clearCachedSchema();
                resolve(new DatabaseResult(db, this, newSchema));
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
                var sql = buildSelect(this, null, null, null, values, db.definedTableRelationships(), schemaResult.data);
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

    public function page(pageIndex:Int, pageSize:Int = 100, query:QueryExpr = null, allowRelationships:Bool = true):Promise<DatabaseResult<Array<Record>>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'all'));
                return;
            }

            refreshSchema().then(schemaResult -> {
                var relationshipDefinintions = db.definedTableRelationships();
                if (!allowRelationships) {
                    relationshipDefinintions = null;
                }

                var values = [];
                var sql = buildSelect(this, null, pageSize, pageIndex * pageSize, values, relationshipDefinintions, schemaResult.data);
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

    private var retryCount = 0;
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
                return nativeDB.run(sql, values);
            }).then(result -> {
                if (result != null && result.data != null) {
                    insertedId = result.data.lastID;
                    record.field("_insertedId", insertedId);
                    resolve(new DatabaseResult(db, this, record));
                } else {
                    resolve(new DatabaseResult(db, this, record));
                }
            }, (error:SqliteError) -> {
                if (error.message.contains("SQLITE_BUSY")) { // bit of a sneaky way to avoid busy errors, if you them consitently, you are using sqlite for the wrong thing! 
                    retryCount++;
                    if (retryCount < 5) {
                        haxe.Timer.delay(() -> {
                            add(record).then(result -> {
                                resolve(result);
                            }, error -> {
                                reject(error);
                            });
                        }, 20);
                    } else {
                        reject(SqliteError2DatabaseError(error, "add"));
                    }
                } else {
                    reject(SqliteError2DatabaseError(error, "add"));
                }
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
                return nativeDB.exec("VACUUM;");
            }).then(response -> {
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

    public function find(query:QueryExpr, allowRelationships:Bool = true):Promise<DatabaseResult<Array<Record>>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'find'));
                return;
            }

            refreshSchema().then(schemaResult -> {
                var values = [];
                var relationshipDefinintions = db.definedTableRelationships();
                if (!allowRelationships) {
                    relationshipDefinintions = null;
                }
                var sql = buildSelect(this, query, null, null, values, relationshipDefinintions, schemaResult.data);
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

    public function findOne(query:QueryExpr, allowRelationships:Bool = true):Promise<DatabaseResult<Record>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'findOne'));
                return;
            }

            refreshSchema().then(schemaResult -> {
                var values = [];
                var relationshipDefinintions = db.definedTableRelationships();
                if (!allowRelationships) {
                    relationshipDefinintions = null;
                }
                var sql = buildSelect(this, query, 1, null, values, relationshipDefinintions, schemaResult.data);
                return nativeDB.all(sql, values);
            }).then(response -> {
                resolve(new DatabaseResult(db, this, Record.fromDynamic(response.data[0])));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "findOne"));
            });
        });
    }

    public function findUnique(columnName:String, query:QueryExpr = null, allowRelationships:Bool = true):Promise<DatabaseResult<Array<Record>>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'find'));
                return;
            }

            refreshSchema().then(schemaResult -> {
                var values = [];
                var relationshipDefinintions = db.definedTableRelationships();
                if (!allowRelationships) {
                    relationshipDefinintions = null;
                }
                var sql = buildDistinctSelect(this, query, columnName, null, null, values, relationshipDefinintions, schemaResult.data);
                return nativeDB.all(sql, values);
            }).then(response -> {
                var records = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                resolve(new DatabaseResult(db, this, records));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "findUnique"));
            });
        });
    }

    public function count(query:QueryExpr = null):Promise<DatabaseResult<Int>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'find'));
                return;
            }
            
            refreshSchema().then(schemaResult -> {
                var sql = 'SELECT COUNT(*) from ${this.name}';
                return nativeDB.get(sql);
            }).then(response -> {
                var record = Record.fromDynamic(response.data);
                resolve(new DatabaseResult(db, this, cast record.values()[0]));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "findUnique"));
            });
        });
    }

    public function addColumn(column:ColumnDefinition):Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'addColumn'));
                return;
            }

            var sql = buildAddColumns(this.name, [column], SqliteDataTypeMapper.get());
            nativeDB.exec(sql).then(result -> {
                clearCachedSchema();
                cast(db, SqliteDatabase).clearCachedSchema();
                resolve(new DatabaseResult(db, this, true));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "addColumn"));
            });
        });
    }

    public function removeColumn(column:ColumnDefinition):Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'addColumn'));
                return;
            }

            var sql = buildRemoveColumns(this.name, [column], SqliteDataTypeMapper.get());
            nativeDB.exec(sql).then(result -> {
                clearCachedSchema();
                cast(db, SqliteDatabase).clearCachedSchema();
                resolve(new DatabaseResult(db, this, true));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "addColumn"));
            });
        });
    }

    private var nativeDB(get, null):NativeDatabase;
    private function get_nativeDB():NativeDatabase {
        return @:privateAccess cast(db, SqliteDatabase)._db;
    }

    private function refreshSchema():Promise<DatabaseResult<DatabaseSchema>> { // we'll only refresh the data schema if there are table relationships, since the queries might need them
        return new Promise((resolve, reject) -> {
            var alwaysAliasResultFields:Bool = this.db.getProperty("alwaysAliasResultFields", false);
            if (alwaysAliasResultFields == false && db.definedTableRelationships() == null) {
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