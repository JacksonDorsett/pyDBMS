from typing import Text
from pydb.dbtype import *

class SimpleModel(Model):
    __table_name__ = 'simple_model'
    __primary_keys__ = ['model_id']
    model_id = String()
    integer_column = Integer()
    float_column = Float()

class SimpleTextModel(Model):
    __table_name__ = 'simple_model'
    __primary_keys__ = ['model_id']

    model_id = CharN(5)
    boolean_column = Boolean()

class SimpleChildModel(SimpleModel):
    __table_name__ = 'simple_child_model'
    other_column = Integer()

class NonNullableModel(Model):
    __table_name__ = 'non_nullable_model'
    __primary_keys__ = 'model_id'
    model_id = String(is_nullable=False)

class StringPrimaryKeyModel(Model):
    __table_name__ = 'string_primary_key_model'
    __primary_keys__ = 'model_id'
    model_id = String()

class CharNModel(Model):
    __table_name__ ='charn_model'
    __primary_keys__ = 'model_id'

    model_id = CharN(10)

class NoPrimaryKeyModel(Model):
    __table_name__ = 'missing_pk'

    model_id = String()