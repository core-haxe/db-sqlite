package db.sqlite;

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

    public function haxeTypeToDatabaseType(haxeType:ColumnType):String {
        return switch (haxeType) {
            case Number:        'INTEGER';
            case Decimal:       'DECIMAL';
            case Boolean:       'INTEGER';
            case Text(n):       'VARCHAR($n)';
            case Memo:          'TEXT';
            case Binary:        'BLOB';
        }
    }
}