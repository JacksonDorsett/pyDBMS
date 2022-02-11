from pyDBMS.dbtype import Boolean, DBType, Integer, String, Float, DateTime, Date


class TypeMapper():
    type_mapping = {
        'INTEGER' : Integer,
        'TEXT' : String,
        'FLOAT' : Float
    }
    def get_type(self, type_string) -> DBType:
        return self.type_mapping.get(type_string)


class PostgresTypeMapper(TypeMapper):
    type_mapping = {
        'timestamp without time zone' : DateTime,
        'boolean' : Boolean,
        'integer' : Integer,
        'float' : Float,
        'text' : String,
        'double precision' : Float,
        'date' : Date

    }