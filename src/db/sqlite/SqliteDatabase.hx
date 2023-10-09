package db.sqlite;

import db.sqlite.Utils.*;
import promises.Promise;
import sqlite.Database as NativeDatabase;
import sqlite.SqliteError;
import sqlite.SqliteOpenMode;
import sys.FileSystem;
import sys.io.File;

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
                _relationshipDefs.complexRelationships = value;
            }
        }
        _properties.set(name, value);
    }
    public function getProperty(name:String, defaultValue:Any):Any {
        if (_properties == null || !_properties.exists(name)) {
            return defaultValue;
        }
        return _properties.get(name);
    }

    private var _schema:DatabaseSchema = null;
    public function schema():Promise<DatabaseResult<DatabaseSchema>> {
        return new Promise((resolve, reject) -> {
            if (_schema == null) {
                Utils.loadFullDatabaseSchema(_db).then(schema -> {
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

    public function connect():Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            if (!FileSystem.exists(_config.filename)) {
                File.saveContent(_config.filename, "");
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
                /*
                schema().then(schemaResult -> {
                    resolve(new DatabaseResult(this, response.data));
                }, (schemaError:DatabaseError) -> {
                    reject(schemaError);
                });
                */
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
            _db = null;
            resolve(new DatabaseResult(this, true));
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
                    tableCache.set(name, table);
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
}