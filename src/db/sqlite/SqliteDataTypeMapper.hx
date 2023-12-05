package db.sqlite;

using StringTools;

class SqliteDataTypeMapper implements IDataTypeMapper {
    private static var _instance:IDataTypeMapper = null;
    public static function get():IDataTypeMapper {
        if (_instance == null) {
            _instance = new SqliteDataTypeMapper();
        }
        return _instance;
    }

    ///////////////////////////////////////////////////////////////////////////////////////

    public function new() {
    }

    public function shouldConvertValueToDatabase(value:Any):Bool {
        return false;
    }

    public function convertValueToDatabase(value:Any):Any {
        return value;
    }

    public function haxeTypeToDatabaseType(haxeType:ColumnType):String {
        return switch (haxeType) {
            case Number:        'INTEGER';
            case Decimal:       'DECIMAL';
            case Boolean:       'INTEGER';
            case Text(n):       'VARCHAR($n)';
            case Memo:          'TEXT';
            case Binary:        'BLOB';
            case Unknown:       'TEXT';
        }
    }

    public function databaseTypeToHaxeType(databaseType:String):ColumnType {
        databaseType = databaseType.toUpperCase();
        if (databaseType == "INTEGER") {
            return Number;
        }
        if (databaseType == "DECIMAL") {
            return Decimal;
        }
        if (databaseType.startsWith("VARCHAR")) {
            var count = databaseType.replace("VARCHAR", "").replace("(", "").replace(" ", "");
            return Text(Std.parseInt(count));
        }
        if (databaseType == "TEXT") {
            return Memo;
        }
        if (databaseType == "BLOB") {
            return Binary;
        }
        return Memo;
    }
}