import unittest
from pydb.dbtype import *
from .example_types import *
class TestInitModel(unittest.TestCase):

    def test_fields_on_empty_model(self):
        model = SimpleModel()
        self.assertListEqual(sorted(model.fields), sorted(['model_id', 'integer_column','float_column']))

    def test_fields_on_model_with_two_parents(self):
        model = SimpleChildModel()
        self.assertListEqual(sorted(model.fields), sorted(['model_id', 'integer_column','float_column','other_column']))

    def test_set_value_on_simple_model(self):
        model = SimpleModel()
        model['model_id'] = 'test_id'
        self.assertEqual(model['model_id'], 'test_id')

    def test_set_invalid_field_on_simple_model(self):
        model = SimpleModel()
        with self.assertRaises(KeyError):
            model['invalid_column'] = 100

    def test_set_invalid_field_type_on_simple_model(self):
        model = SimpleModel()
        with self.assertRaises(ValueError):
            model['integer_column'] = 'ab'


    def test_set_invalid_field_on_child_model(self):
        model = SimpleChildModel()
        with self.assertRaises(KeyError):
            model['invalid_column'] = 100

    def test_set_invalid_field_type_on_child_model(self):
        model = SimpleChildModel()
        with self.assertRaises(ValueError):
            model['other_column'] = 'ab'

    def test_init_model_with_kwargs(self):
        model = SimpleModel(model_id='test_id',float_column=1.5,integer_column=10)
        self.assertEqual(model['model_id'], 'test_id')
        self.assertEqual(model['float_column'], 1.5)
        self.assertEqual(model['integer_column'],10)

    def test_init_model_with_invalid_kwargs_fields(self):
        with self.assertRaises(KeyError):
            model = SimpleModel(invalid_column=100)
    
    def test_init_model_with_invalid_kwargs_values(self):
        with self.assertRaises(ValueError):
            model = SimpleModel(integer_column='ab')

    def test_set_value_with_null_field(self):
        model = SimpleModel()
        model['model_id'] = None

    def test_set_value_with_null_field_for_non_nullable_field(self):
        model = NonNullableModel()
        with self.assertRaises(ValueError):
            model['model_id'] = None

    def test_model_with_primary_key_as_string(self):
        model = StringPrimaryKeyModel(model_id = 'test_id')
        self.assertEqual(model.__primary_keys__, ['model_id'])

    def test_model_set_value_with_finite_length(self):
        model = CharNModel()
        model['model_id'] = 'abcdefghijklmnopqrstuvwxyz'
        self.assertEqual(model['model_id'], 'abcdefghij')