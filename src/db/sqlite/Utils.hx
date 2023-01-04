package db.sqlite;

import sqlite.SqliteError;

class Utils {
    public static inline var SQL_TABLE_EXISTS = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;";

    public static function SqliteError2DatabaseError(error:SqliteError, call:String):DatabaseError {
        var dbError = new DatabaseError(error.message, call);
        return dbError;
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