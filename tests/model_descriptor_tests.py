import unittest
from pydb.database.model_descriptor import StandardModelDescriptor
from pydb.dbtype import DBType, Model
from tests.example_types import SimpleModel, NonNullableModel

class InvalidType(DBType):
    _python_type = None

class InvalidModel(Model):
    __table_name__ = 'invalid_table'
    __primary_keys__ = 'model_id'

    model_id = InvalidType()

class TestModelDescriptor(unittest.TestCase):
    
    def test_describe_model(self):
        descriptor = StandardModelDescriptor()
        result = descriptor.describe(SimpleModel())

        expected_result = '''CREATE TABLE simple_model (
float_column FLOAT,
integer_column INTEGER,
model_id TEXT,
PRIMARY KEY (model_id)
)'''
        self.assertIsInstance(result, str)
        self.assertEqual(result, expected_result)

    def test_describe_model_with_not_null(self):
        descriptor = StandardModelDescriptor()
        result = descriptor.describe(NonNullableModel())
        expected_result = '''CREATE TABLE non_nullable_model (
model_id TEXT NOT NULL,
PRIMARY KEY (model_id)
)'''
        self.assertIsInstance(result, str)
        self.assertEqual(result, expected_result)

    def test_describe_model_for_unsupported_type(self):
        descriptor = StandardModelDescriptor()
        with self.assertRaises(NotImplementedError):
            descriptor.describe(InvalidModel())
