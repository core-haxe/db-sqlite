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

    public static function loadFullDatabaseSchema(db:NativeDatabase, typeMapper:IDataTypeMapper):Promise<DatabaseSchema> {
        return new Promise((resolve, reject) -> {
            var schema:DatabaseSchema = {};
            
            #if (cpp || !sys)

            var tablesWithAutoIncrement:Array<String> = [];
            db.all("SELECT * FROM SQLITE_SEQUENCE").then(results -> {
                for (r in results.data) {
                    tablesWithAutoIncrement.push(r.name);
                }
                return db.all(SQL_LIST_TABLES_AND_FIELDS);
            }).then(results -> {
                for (r in results.data) {
                    if (r.table_name == "sqlite_sequence") {
                        continue;
                    }
                    var table = schema.findTable(r.table_name);
                    if (table == null) {
                        table = {
                            name: r.table_name
                        };
                        schema.tables.push(table);
                    }

                    var options = [];
                    if (r.pk == 1) {
                        options.push(ColumnOptions.PrimaryKey);
                        if (tablesWithAutoIncrement.contains(table.name)) {
                            options.push(ColumnOptions.AutoIncrement);
                        }
                    }
                    if (r.notnull == 1) {
                        options.push(ColumnOptions.NotNull);
                    }
                    var sqliteType = r.type;
                    table.columns.push({
                        name: r.name,
                        type: typeMapper.databaseTypeToHaxeType(sqliteType),
                        options: options
                    });
                }
                resolve(schema);
            }, (error:SqliteError) -> {
                reject(error);
            });

            #else // good old sys db cant use "SQL_LIST_TABLES_AND_FIELDS" because "reasons" so we'll manually parse the reasults of "SELECT * FROM sqlite_master"

            var tablesWithAutoIncrement:Array<String> = [];
            db.all("SELECT * FROM SQLITE_SEQUENCE").then(results -> {
                for (r in results.data) {
                    tablesWithAutoIncrement.push(r.name);
                }
                return db.all("SELECT * FROM sqlite_master WHERE type = 'table';");
            }).then(results -> {
                for (r in results.data) {
                    if (r.tbl_name == "sqlite_sequence") {
                        continue;
                    }
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
                            f = f.replace("PRIMARY KEY", "PRIMARYKEY");
                            f = f.replace("NOT NULL", "NOTNULL");
                            var parts = f.split(" ");

                            var fieldName = StringTools.trim(parts[0]);
                            if (fieldName.length == 0) {
                                continue;
                            }
                            var fieldName = parts.shift();
                            fieldName = fieldName.replace("`", "");

                            var fieldType = parts.shift();

                            var options = [];
                            if (parts.contains("PRIMARYKEY")) {
                                options.push(ColumnOptions.PrimaryKey);
                                if (tablesWithAutoIncrement.contains(table.name)) {
                                    options.push(ColumnOptions.AutoIncrement);
                                }
                            }
                            if (parts.contains("NOTNULL")) {
                                options.push(ColumnOptions.NotNull);
                            }

                            table.columns.push({
                                name: fieldName,
                                type: typeMapper.databaseTypeToHaxeType(fieldType),
                                options: options
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

    public static function buildAddColumns(tableName:String, columns:Array<ColumnDefinition>, typeMapper:IDataTypeMapper):String {
        var sql = 'ALTER TABLE ${tableName}\n';

        for (column in columns) {
            var type = typeMapper.haxeTypeToDatabaseType(column.type);
            sql += 'ADD `${column.name}` ${type}';
        }

        sql += ';';

        return sql;
    }

    public static function buildRemoveColumns(db:NativeDatabase, tableName:String, columns:Array<ColumnDefinition>, databaseSchema:DatabaseSchema, typeMapper:IDataTypeMapper):Promise<String> {
        return new Promise((resolve, reject) -> {
            #if (hl || neko)

            db.get("SELECT * FROM sqlite_master WHERE type='table' AND name='" + tableName + "';").then(results -> {
                var sql = null;
                var originalSql:String = results.data.sql;
                var newSql = "";
                var lines = originalSql.split("\n");
                for (line in lines) {
                    var temp = line.trim().replace("`", "");
                    var use = true;
    
                    for (c in columns) {
                        if (temp.startsWith(c.name)) {
                            use = false;
                            break;
                        }
                    }
    
                    if (use) {
                        newSql += line + "\n";
                    }
                }
                newSql = newSql.trim();
                if (newSql.endsWith(",")) {
                    newSql = newSql.substring(0, newSql.length - 1);
                }
                if (!newSql.endsWith(")")) {
                    newSql += ")";
                }
                if (!newSql.endsWith(";")) {
                    newSql += ";";
                }
    
                var backupTable = tableName + "_backup";
                sql = 'BEGIN TRANSACTION;\n';
        
                var tableSchema = databaseSchema.findTable(tableName);
                if (tableSchema == null) {
                    throw "could not find table schema";
                }
        
                var columnList = [];
                for (f in tableSchema.columns) {
                    if (!hasColumn(columns, f.name)) {
                        columnList.push(f.name);
                    }
                }

                var columnListString = columnList.join(",");
        
                sql += 'CREATE TEMPORARY TABLE $backupTable($columnListString);\n';
                sql += 'INSERT INTO $backupTable SELECT $columnListString FROM $tableName;\n';
                sql += 'DROP TABLE $tableName;\n';
                //sql += 'CREATE TABLE $tableName($columnListString);\n';
                sql += newSql + "\n";
                sql += 'INSERT INTO $tableName SELECT $columnListString FROM $backupTable;\n';
                sql += 'DROP TABLE $backupTable;\n';
                sql += 'COMMIT;\n';
        
                resolve(sql);
            }, (error:SqliteError) -> {
                trace("error", error.message);
            });
    
            #else
    
            var sql = 'ALTER TABLE ${tableName}\n';
            for (column in columns) {
                sql += 'DROP COLUMN ${column.name}';
            }
            sql += ';';
            resolve(sql);
    
            #end
        });
    }

    private static function hasColumn(columns:Array<ColumnDefinition>, columnName:String) {
        for (c in columns) {
            if (c.name == columnName) {
                return true;
            }
        }
        return false;
    }
}