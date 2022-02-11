
from typing import Type

from pyDBMS.dbtype import DBType, Integer, String, Float


class TypeMapper():
    type_mapping = {
        'INTEGER' : Integer,
        'TEXT' : String,
        'FLOAT' : Float
    }
    def get_type(self, type_string) -> DBType:
        return self.type_mapping.get(type_string)