package db.sqlite;

import db.sqlite.Utils.*;
import promises.Promise;
import sqlite.Database as NativeDatabase;
import sqlite.SqliteError;
import sqlite.SqliteOpenMode;

class SqliteDatabase implements IDatabase {
    private var _db:NativeDatabase;
    private var _relationshipDefs:RelationshipDefinitions = null;

    private var _config:Dynamic;

    public function new() {
    }

    public function config(details:Dynamic) {
        // TODO: validate
        _config = details;
        if (_config != null && _config.journalMode != null) {
            setProperty("journalMode", _config.journalMode);
        }
    }

    // TODO: combine with config?
    private var _properties:Map<String, Any> = [];
    public function setProperty(name:String, value:Any) {
        if (name == "complexRelationships") {
            if (_relationshipDefs == null) {
                _relationshipDefs = new RelationshipDefinitions();
            }
            _relationshipDefs.complexRelationships = value;
        }
        _properties.set(name, value);
    }
    public function getProperty(name:String, defaultValue:Any):Any {
        if (_properties == null || !_properties.exists(name)) {
            return defaultValue;
        }
        return _properties.get(name);
    }

    public function create():Promise<DatabaseResult<IDatabase>> {
        return new Promise((resolve, reject) -> {
            resolve(new DatabaseResult(this, null, cast this));
        });
    }

    public function delete():Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            if (!sys.FileSystem.exists(_config.filename)) {
                resolve(new DatabaseResult(this, null, true));
            } else {
                disconnect().then(_ -> {
                    if (sys.FileSystem.exists(_config.filename)) {
                        sys.FileSystem.deleteFile(_config.filename);
                    }
                    return connect();
                }).then(_ -> {
                    resolve(new DatabaseResult(this, null, true));
                }, (error:SqliteError) -> {
                    reject(SqliteError2DatabaseError(error, "delete"));
                });
            }
        });
    }

    private var _schema:DatabaseSchema = null;
    public function schema(force:Bool = false):Promise<DatabaseResult<DatabaseSchema>> {
        return new Promise((resolve, reject) -> {
            if (force) {
                clearCachedSchema();
            }
            if (_schema == null) {
                Utils.loadFullDatabaseSchema(_db, SqliteDataTypeMapper.get()).then(schema -> {
                    _schema = schema;
                    resolve(new DatabaseResult(this, _schema));
                }, (error:SqliteError) -> {
                    reject(SqliteError2DatabaseError(error, "schema"));
                });
            } else {
                resolve(new DatabaseResult(this, _schema));
            }
        });
    }

    public function clearCachedSchema() {
        _schema = null;
    }

    public function defineTableRelationship(field1:String, field2:String) {
        if (_relationshipDefs == null) {
            _relationshipDefs = new RelationshipDefinitions();
        }
        _relationshipDefs.add(field1, field2);
    }

    public function definedTableRelationships():RelationshipDefinitions {
        return _relationshipDefs;
    }

    public function clearTableRelationships() {
        _relationshipDefs = null;
    }

    public function connect():Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            if (!sys.FileSystem.exists(_config.filename)) {
                sys.io.File.saveContent(_config.filename, "");
            }
            var openMode:SqliteOpenMode = SqliteOpenMode.ReadWrite;
            if (_config.openMode != null) {
                if (_config.openMode == "ReadWrite") {
                    openMode = SqliteOpenMode.ReadWrite;
                } else if (_config.openMode == "ReadOnly") {
                    openMode = SqliteOpenMode.ReadOnly;
                }
            }
            _db = new NativeDatabase(_config.filename, openMode);
            _db.open().then(response -> {
                if (_properties.exists("journalMode")) {
                    _db.run("PRAGMA journal_mode=" + _properties.get("journalMode") + ";").then(_ -> {
                        resolve(new DatabaseResult(this, response.data));
                    });
                } else {
                    resolve(new DatabaseResult(this, response.data));
                }
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "connect"));
            });
        });
    }

    public function disconnect():Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            _db.close().then(_ -> {
                _db = null;
                tableCache = [];
                clearCachedSchema();
                resolve(new DatabaseResult(this, true));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "disconnect"));
            });
        });
    }

    private var tableCache:Map<String, ITable> = [];
    public function table(name:String):Promise<DatabaseResult<ITable>> {
        return new Promise((resolve, reject) -> {
            if (tableCache.exists(name)) {
                resolve(new DatabaseResult(this, tableCache.get(name)));
            } else {
                _db.get(SQL_TABLE_EXISTS, name).then(response -> {
                    var table:ITable = new SqliteTable(this);
                    table.name = name;
                    table.exists = !(response.data == null);

                    #if !sqlite_no_table_cache
                    if (table.exists) {
                        tableCache.set(name, table);
                    }
                    #end

                    resolve(new DatabaseResult(this, table));
                }, (error:SqliteError) -> {
                    reject(SqliteError2DatabaseError(error, "table"));
                });
            }
        });
    }

    public function createTable(name:String, columns:Array<ColumnDefinition>):Promise<DatabaseResult<ITable>> {
        return new Promise((resolve, reject) -> {
            var sql = buildCreateTable(name, columns, SqliteDataTypeMapper.get());
            _db.exec(sql).then(response -> {
                var table:ITable = new SqliteTable(this);
                table.name = name;
                table.exists = true;

                #if !sqlite_no_table_cache
                if (table.exists) {
                    tableCache.set(name, table);
                }
                #end

                _schema = null;
                resolve(new DatabaseResult(this, table));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "createTable"));
            });
        });
    }

    public function deleteTable(name:String):Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            _schema = null;
            reject(new DatabaseError("not implemented", "deleteTable"));
        });
    }

    #if allow_raw
    public function raw(data:String, values:Array<Any> = null):Promise<DatabaseResult<RecordSet>> {
        return new Promise((resolve, reject) -> {
            if (values == null) {
                values = [];
            }
            var sql = data;
            _db.all(sql, values).then(response -> {
                var records:RecordSet = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                resolve(new DatabaseResult(this, records));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "raw"));
            });
        });
    }
    #end
}