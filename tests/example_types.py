from pydb.dbtype import *

class SimpleModel(Model):
    __table_name__ = 'simple_model'
    __primary_keys__ = ['model_id']
    model_id = Text()
    integer_column = Integer()
    float_column = Float()

class SimpleChildModel(SimpleModel):
    __table_name__ = 'simple_child_model'
    other_column = Integer()

class NonNullableModel(Model):
    model_id = Text(is_nullable=False)
