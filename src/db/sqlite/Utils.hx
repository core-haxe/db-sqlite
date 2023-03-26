package db.sqlite;

import promises.Promise;
import sqlite.Database as NativeDatabase;
import sqlite.SqliteError;

using StringTools;

class Utils {
    public static inline var SQL_TABLE_EXISTS = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;";
    public static inline var SQL_LIST_TABLES = "SELECT name FROM sqlite_master WHERE type='table';";
    public static inline var SQL_LIST_FIELDS = "SELECT * FROM pragma_table_info('{tableName}') AS tblInfo;";
    public static inline var SQL_LIST_TABLES_AND_FIELDS = "WITH all_tables AS (SELECT name FROM sqlite_master WHERE type = 'table') 
                                                           SELECT at.name table_name, pti.*
                                                           FROM all_tables at INNER JOIN pragma_table_info(at.name) pti
                                                           ORDER BY table_name;";
    public static inline var SQL_LAST_INSERTED_ID = "SELECT seq FROM sqlite_sequence WHERE name=?;";

    public static function SqliteError2DatabaseError(error:SqliteError, call:String):DatabaseError {
        var dbError = new DatabaseError(error.message, call);
        return dbError;
    }

    public static function loadFullDatabaseSchema(db:NativeDatabase):Promise<DatabaseSchema> {
        return new Promise((resolve, reject) -> {
            var schema:DatabaseSchema = {};
            
            #if !sys

            db.all(SQL_LIST_TABLES_AND_FIELDS).then(results -> {
                for (r in results.data) {
                    var table = schema.findTable(r.table_name);
                    if (table == null) {
                        table = {
                            name: r.table_name
                        };
                        schema.tables.push(table);
                    }
                    table.columns.push({
                        name: r.name,
                        type: null
                    });
                }
                resolve(schema);
            }, (error:SqliteError) -> {
                reject(error);
            });

            #else // good old sys db cant use "SQL_LIST_TABLES_AND_FIELDS" because "reasons" so we'll manually parse the reasults of "SELECT * FROM sqlite_master"

            db.all("SELECT * FROM sqlite_master WHERE type = 'table';").then(results -> {
                for (r in results.data) {
                    var table = schema.findTable(r.tbl_name);
                    if (table == null) {
                        table = {
                            name: r.tbl_name
                        };
                        schema.tables.push(table);
                    }
                    
                    var sql:String = r.sql;
                    var n1 = sql.indexOf("(");
                    var n2 = sql.lastIndexOf(")");
                    if (n1 != -1 && n2 != -1) {
                        var fieldListString = sql.substring(n1 + 1, n2);
                        var fieldList = fieldListString.split(",");
                        for (f in fieldList) {
                            f = StringTools.trim(f);
                            var parts = f.split(" ");
                            var fieldName = StringTools.trim(parts[0]);
                            if (fieldName.length == 0) {
                                continue;
                            }
                            var fieldName = parts[0];
                            fieldName = fieldName.replace("`", "");
                            table.columns.push({
                                name: fieldName,
                                type: null
                            });
                        }
                    }
                }
                
                resolve(schema);
            }, (error:SqliteError) -> {
                reject(error);
            });

            #end
        });
    }

    public static function buildCreateTable(tableName:String, columns:Array<ColumnDefinition>, typeMapper:IDataTypeMapper) {
        var sql = 'CREATE TABLE ${tableName} (\n';

        var columnParts = [];
        var unique:Array<String> = null;
        for (column in columns) {
            var type = typeMapper.haxeTypeToDatabaseType(column.type);
            var columnSql = '    `${column.name}`';
            columnSql += ' ${type}';
            if (column.options != null) {
                if (column.options.contains(PrimaryKey)) {
                    if (unique == null) {
                        columnSql += ' PRIMARY KEY';
                        unique = [];
                        unique.push(column.name);
                    } else {
                        unique.push(column.name);
                    }
                }
                if (column.options.contains(AutoIncrement)) {
                    columnSql += ' AUTOINCREMENT';
                }
                if (column.options.contains(NotNull)) {
                    columnSql += ' NOT NULL';
                }
            }

            columnParts.push(columnSql);
        }

        sql += columnParts.join(",\n");

        if (unique != null && unique.length > 1) {
            sql += ",\n    UNIQUE (";
            sql += unique.join(", ");
            sql += ")\n";
        }
        sql += ');';
        return sql;
    }
}