package db.sqlite;

import promises.Promise;
import sqlite.SqliteError;

import sqlite.Database as NativeDatabase;

class Utils {
    public static inline var SQL_TABLE_EXISTS = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;";
    public static inline var SQL_LIST_TABLES = "SELECT name FROM sqlite_master WHERE type='table';";
    public static inline var SQL_LIST_FIELDS = "SELECT * FROM pragma_table_info('{tableName}') AS tblInfo;";
    public static inline var SQL_LIST_TABLES_AND_FIELDS = "WITH all_tables AS (SELECT name FROM sqlite_master WHERE type = 'table') 
                                                           SELECT at.name table_name, pti.*
                                                           FROM all_tables at INNER JOIN pragma_table_info(at.name) pti
                                                           ORDER BY table_name;";

    public static function SqliteError2DatabaseError(error:SqliteError, call:String):DatabaseError {
        var dbError = new DatabaseError(error.message, call);
        return dbError;
    }

    public static function loadFullDatabaseSchema(db:NativeDatabase):Promise<DatabaseSchema> {
        return new Promise((resolve, reject) -> {
            var schema:DatabaseSchema = {};
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
        });
    }

    public static function buildCreateTable(tableName:String, columns:Array<ColumnDefinition>, typeMapper:IDataTypeMapper) {
        var sql = 'CREATE TABLE ${tableName} (\n';

        var columnParts = [];
        for (column in columns) {
            var type = typeMapper.haxeTypeToDatabaseType(column.type);
            var columnSql = '    ${column.name}';
            columnSql += ' ${type}';
            if (column.options != null) {
                if (column.options.contains(PrimaryKey)) {
                    columnSql += ' PRIMARY KEY';
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

        sql += ');';
        return sql;
    }
}