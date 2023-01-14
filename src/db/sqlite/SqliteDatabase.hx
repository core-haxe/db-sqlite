package db.sqlite;

import promises.Promise;
import sqlite.SqliteError;
import sqlite.Database as NativeDatabase;
import db.sqlite.Utils.*;

class SqliteDatabase implements IDatabase {
    private var _db:NativeDatabase;
    private var _relationshipDefs:RelationshipDefinitions = null;

    private var _config:Dynamic;

    public function new() {
    }

    public function config(details:Dynamic) {
        // TODO: validate
        _config = details;
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
            _db = new NativeDatabase(_config.filename);
            _db.open().then(response -> {
                resolve(new DatabaseResult(this, response.data));
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

    public function table(name:String):Promise<DatabaseResult<ITable>> {
        return new Promise((resolve, reject) -> {
            _db.get(SQL_TABLE_EXISTS, name).then(response -> {
                var table:ITable = new SqliteTable(this);
                table.name = name;
                table.exists = !(response.data == null);
                resolve(new DatabaseResult(this, table));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "table"));
            });
        });
    }

    public function createTable(name:String, columns:Array<ColumnDefinition>):Promise<DatabaseResult<ITable>> {
        return new Promise((resolve, reject) -> {
            var sql = buildCreateTable(name, columns, SqliteDataTypeMapper.get());
            _db.exec(sql).then(response -> {
                var table:ITable = new SqliteTable(this);
                table.name = name;
                table.exists = true;
                resolve(new DatabaseResult(this, table));
            }, (error:SqliteError) -> {
                reject(SqliteError2DatabaseError(error, "createTable"));
            });
        });
    }

    public function deleteTable(name:String):Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            reject(new DatabaseError("not implemented", "deleteTable"));
        });
    }
}