from pydb.dbtype import *

class SimpleModel(Model):
    __table_name__ = 'simple_model'
    __primary_keys__ = ['model_id']
    model_id = String()
    integer_column = Integer()
    float_column = Float()

class SimpleChildModel(SimpleModel):
    __table_name__ = 'simple_child_model'
    other_column = Integer()

class NonNullableModel(Model):
    model_id = String(is_nullable=False)

class StringPrimaryKeyModel(Model):
    __table_name__ = 'string_primary_key_model'
    __primary_keys__ = 'model_id'
    model_id = String()

class CharNModel(Model):
    __table_name__ ='charn_model'
    __primary_keys__ = 'model_id'

    model_id = CharN(10)