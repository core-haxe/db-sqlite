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
    public function schema(force:Bool = false):Promise<DatabaseResult<TableSchema>> {
        return new Promise((resolve, reject) -> {
            if (force) {
                clearCachedSchema();
            }
            if (_tableSchema != null) {
                resolve(new DatabaseResult(db, this, _tableSchema));
                return;
            }

            this.db.schema(force).then(result -> {
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
            
            var schemaChanged:Bool = false;

            // we always want to force refresh the schema, so we are working with the latest, not a cached copy
            schema(true).then(result -> {
                var promises = [];
                var currentSchema = result.data;
                if (currentSchema != null && !currentSchema.equals(newSchema)) {
                    var diff = currentSchema.diff(newSchema);

                    for (added in diff.addedColumns) {
                        promises.push(addColumn.bind(added));
                        schemaChanged = true;
                    }

                    for (removed in diff.removedColumns) {
                        promises.push(removeColumn.bind(removed));
                        schemaChanged = true;
                    }
                }
                return PromiseUtils.runSequentially(promises);
            }).then(result -> {
                if (schemaChanged) {
                    clearCachedSchema();
                    cast(db, SqliteDatabase).clearCachedSchema();
                }
                resolve(new DatabaseResult(db, this, newSchema));
            }, (error:DatabaseError) -> {
                reject(error);
            });
        });
    }

    public function createIndex(fields:Array<String>, unique:Bool = false, name:String = null):Promise<DatabaseResult<ITable>> {
        return new Promise((resolve, reject) -> {
            var sql = buildCreateIndex(this, fields, unique, name);
            nativeDB.exec(sql).then(result -> {
                resolve(new DatabaseResult(db, this));
            }, (error:DatabaseError) -> {
                reject(error);
            });
        });
    }

    public function all():Promise<DatabaseResult<RecordSet>> {
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
                var records:RecordSet = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                resolve(new DatabaseResult(db, this, records));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "all"));
            });
        });
    }

    public function page(pageIndex:Int, pageSize:Int = 100, query:QueryExpr = null, allowRelationships:Bool = true):Promise<DatabaseResult<RecordSet>> {
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
                var sql = buildSelect(this, query, pageSize, pageIndex * pageSize, values, relationshipDefinintions, schemaResult.data);
                return nativeDB.all(sql, values);
            }).then(response -> {
                var records:RecordSet = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                resolve(new DatabaseResult(db, this, records));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "all"));
            });
        });
    }

    private var retryCountAdd = 0;
    public function add(record:Record):Promise<DatabaseResult<Record>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'add'));
                return;
            }
            
            var values = [];
            var insertedId:Int = -1;
            var sql = buildInsert(this, record, values, SqliteDataTypeMapper.get());
            var schema:DatabaseSchema = null;
            refreshSchema(true).then(result -> {
                schema = result.data;
                return nativeDB.run(sql, values);
            }).then(result -> {
                retryCountAdd = 0;
                if (result != null && result.data != null) {
                    insertedId = result.data.lastID;
                    record.field("_insertedId", insertedId);

                    var tableSchema = schema.findTable(this.name);
                    if (tableSchema != null) {
                        var primaryKeyColumns = tableSchema.findPrimaryKeyColumns();
                        if (primaryKeyColumns.length == 1) { // we'll only "auto set" the primary key column if there is _only_ one of them
                            record.field(primaryKeyColumns[0].name, insertedId);
                        }
                    }

                    resolve(new DatabaseResult(db, this, record, result.changes));
                } else {
                    resolve(new DatabaseResult(db, this, record, result.changes));
                }
            }, (error:SqliteError) -> {
                if (error.message.contains("SQLITE_BUSY")) { // bit of a sneaky way to avoid busy errors, if you them consistently, you are using sqlite for the wrong thing! 
                    retryCountAdd++;
                    trace("SQLITE_BUSY in SqliteTable::add, attempting to retry operation (" + retryCountAdd + ")");
                    if (retryCountAdd < 5) {
                        haxe.Timer.delay(() -> {
                            add(record).then(result -> {
                                resolve(result);
                            }, error -> {
                                reject(error);
                            });
                        }, 50);
                    } else {
                        reject(SqliteError2DatabaseError(error, "add"));
                    }
                } else {
                    reject(SqliteError2DatabaseError(error, "add"));
                }
            });
        });
    }

    public function addAll(records:RecordSet):Promise<DatabaseResult<RecordSet>> {
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
                var itemsAffected = 0;
                for (result in results) {
                    itemsAffected += result.itemsAffected;
                }
                resolve(new DatabaseResult(db, this, records, itemsAffected));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "addAll"));
            });
        });
    }

    private var retryCountDelete:Int = 0;
    public function delete(record:Record):Promise<DatabaseResult<Record>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'delete'));
                return;
            }
            var values = [];
            var sql = buildDeleteRecord(this, record, values);
            nativeDB.run(sql, values).then(response -> {
                retryCountDelete = 0;
                resolve(new DatabaseResult(db, this, record, response.changes));
            }, (error:SqliteError) -> {
                if (error.message.contains("SQLITE_BUSY")) { // bit of a sneaky way to avoid busy errors, if you them consistently, you are using sqlite for the wrong thing! 
                    retryCountDelete++;
                    trace("SQLITE_BUSY in SqliteTable::delete, attempting to retry operation (" + retryCountDelete + ")");
                    if (retryCountDelete < 5) {
                        haxe.Timer.delay(() -> {
                            delete(record).then(result -> {
                                resolve(result);
                            }, error -> {
                                reject(error);
                            });
                        }, 50);
                    } else {
                        reject(SqliteError2DatabaseError(error, "delete"));
                    }
                } else {
                    reject(SqliteError2DatabaseError(error, "delete"));
                }
            });
        });
    }

    private var retryCountDeleteAll:Int = 0;
    public function deleteAll(query:QueryExpr = null):Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'deleteAll'));
                return;
            }
            var itemsAffected:Null<Int> = null;
            nativeDB.run(buildDeleteWhere(this, query)).then(response -> {
                itemsAffected = response.changes;
                return nativeDB.exec("VACUUM;");
            }).then(response -> {
                retryCountDeleteAll = 0;
                resolve(new DatabaseResult(db, this, true, itemsAffected));
            }, (error:SqliteError) -> {
                if (error.message.contains("SQLITE_BUSY")) { // bit of a sneaky way to avoid busy errors, if you them consistently, you are using sqlite for the wrong thing! 
                    retryCountDeleteAll++;
                    trace("SQLITE_BUSY in SqliteTable::deleteAll, attempting to retry operation (" + retryCountDeleteAll + ")");
                    if (retryCountDeleteAll < 5) {
                        haxe.Timer.delay(() -> {
                            deleteAll(query).then(result -> {
                                resolve(result);
                            }, error -> {
                                reject(error);
                            });
                        }, 50);
                    } else {
                        reject(SqliteError2DatabaseError(error, "deleteAll"));
                    }
                } else {
                    reject(SqliteError2DatabaseError(error, "deleteAll"));
                }
            });
        });
    }

    private var retryCountUpdate:Int = 0;
    public function update(query:QueryExpr, record:Record):Promise<DatabaseResult<Record>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'update'));
                return;
            }
            var values = [];
            var sql = buildUpdate(this, query, record, values, SqliteDataTypeMapper.get());
            nativeDB.run(sql, values).then(result -> {
                retryCountUpdate = 0;
                resolve(new DatabaseResult(db, this, record, result.changes));
            }, (error:SqliteError) -> {
                if (error.message.contains("SQLITE_BUSY")) { // bit of a sneaky way to avoid busy errors, if you them consistently, you are using sqlite for the wrong thing! 
                    retryCountUpdate++;
                    trace("SQLITE_BUSY in SqliteTable::update, attempting to retry operation (" + retryCountUpdate + ")");
                    if (retryCountUpdate < 5) {
                        haxe.Timer.delay(() -> {
                            update(query, record).then(result -> {
                                resolve(result);
                            }, error -> {
                                reject(error);
                            });
                        }, 50);
                    } else {
                        reject(SqliteError2DatabaseError(error, "update"));
                    }
                } else {
                    reject(SqliteError2DatabaseError(error, "update"));
                }
            });
        });
    }

    public function find(query:QueryExpr, allowRelationships:Bool = true):Promise<DatabaseResult<RecordSet>> {
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
                var records:RecordSet = [];
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
                var record:Record = null;
                if (response.data != null && response.data.length > 0) {
                    record = Record.fromDynamic(response.data[0]);
                }
                resolve(new DatabaseResult(db, this, record));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "findOne"));
            });
        });
    }

    public function findUnique(columnName:String, query:QueryExpr = null, allowRelationships:Bool = true):Promise<DatabaseResult<RecordSet>> {
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
                var records:RecordSet = [];
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
                var values = [];
                var sql = buildCount(this, query, values);
                return nativeDB.get(sql, values);
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

            // older versions of sqlite (sys) cant use drop column, so we'll build a script to handle it, something like:
            //
            //     BEGIN TRANSACTION;
            //     CREATE TEMPORARY TABLE Person_backup(personId,lastName,firstName,iconId);
            //     INSERT INTO Person_backup SELECT personId,lastName,firstName,iconId FROM Person;
            //     DROP TABLE Person;
            //     CREATE TABLE Person(personId,lastName,firstName,iconId);
            //     INSERT INTO Person SELECT personId,lastName,firstName,iconId FROM Person_backup;
            //     DROP TABLE Person_backup;
            //     COMMIT;
            //
            // however, we also cant run multiple statements (yay!), so we'll split this string and run it 
            // line by line

            refreshSchema(true).then(schemaResult -> {
                return buildRemoveColumns(nativeDB, this.name, [column], schemaResult.data, SqliteDataTypeMapper.get());
            }).then(sql -> {                    
                var promises = [];
                for (sqlLine in sql.split(";")) {
                    sqlLine = sqlLine.trim();
                    if (sqlLine.length == 0) {
                        continue;
                    }
                    promises.push(nativeDB.exec.bind(sqlLine + ";"));
                }
                return PromiseUtils.runSequentially(promises);
            }).then(response -> {                    
                clearCachedSchema();
                cast(db, SqliteDatabase).clearCachedSchema();
                resolve(new DatabaseResult(db, this, true));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "removeColumn"));
            });
        });
    }

    #if allow_raw
    public function raw(data:String, values:Array<Any> = null):Promise<DatabaseResult<RecordSet>> {
        return new Promise((resolve, reject) -> {
            if (values == null) {
                values = [];
            }
            var sql = data;
            nativeDB.all(sql, values).then(response -> {
                var records:RecordSet = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                resolve(new DatabaseResult(db, this, records));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "raw"));
            });
        });
    }
    #end

    private var nativeDB(get, null):NativeDatabase;
    private function get_nativeDB():NativeDatabase {
        return @:privateAccess cast(db, SqliteDatabase)._db;
    }

    private function refreshSchema(force:Bool = false):Promise<DatabaseResult<DatabaseSchema>> { // we'll only refresh the data schema if there are table relationships, since the queries might need them
        return new Promise((resolve, reject) -> {
            var alwaysAliasResultFields:Bool = this.db.getProperty("alwaysAliasResultFields", false);
            if (force == false && alwaysAliasResultFields == false && db.definedTableRelationships() == null) {
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