from datetime import time
import unittest
from pyDBMS.dbtype import *
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

    def test_set_datetime_with_valid_string(self):
        model = LogTimestamp()
        model['timestamp'] = '2022-01-01'
        self.assertEqual(model['timestamp'], datetime.fromisoformat('2022-01-01'))
    
    def test_set_datetime_with_invalid_string(self):
        model = LogTimestamp()
        with self.assertRaises(ValueError):
            model['timestamp'] = 'invalid_timestamp'
        

    def test_set_datetime_with_float(self):
        expected_datetime = datetime.now()
        timestamp = expected_datetime.timestamp()
        model = LogTimestamp()
        model['timestamp'] = timestamp
        self.assertEqual(model['timestamp'], expected_datetime)
    
    def test_set_datetime_with_integer(self):
        timestamp = 10000000
        expected_datetime = datetime.fromtimestamp(timestamp)

        model = LogTimestamp()
        model['timestamp'] = timestamp
        self.assertEqual(expected_datetime, model['timestamp'])

    def test_set_datetime_with_datetime(self):
        expected_datetime = datetime.now()

        model = LogTimestamp()
        model['timestamp'] = expected_datetime

        self.assertEqual(expected_datetime, model['timestamp'])

    def test_convert_datetime_field(self):
        model = LogTimestamp()
        with self.assertRaises(ValueError):
            model.timestamp._convert('invalid_input')

    
    def test_set_date_with_valid_string(self):
        model = SpecialDate()
        model['timestamp'] = '2022-01-01'
        self.assertEqual(model['timestamp'], date.fromisoformat('2022-01-01'))
    
    def test_set_date_with_invalid_string(self):
        model = SpecialDate()
        with self.assertRaises(ValueError):
            model['timestamp'] = 'invalid_timestamp'
        

    def test_set_date_with_float(self):
        expected_datetime = datetime.now()
        today = expected_datetime.date()
        timestamp = expected_datetime.timestamp()
        model = SpecialDate()
        model['timestamp'] = timestamp
        self.assertEqual(model['timestamp'], today)
    
    def test_set_date_with_integer(self):
        timestamp = 10000000
        expected_date = date.fromtimestamp(timestamp)

        model = SpecialDate()
        model['timestamp'] = timestamp
        self.assertEqual(expected_date, model['timestamp'])

    def test_set_date_with_datetime(self):
        expected_datetime = date.today()

        model = SpecialDate()
        model['timestamp'] = expected_datetime

        self.assertEqual(expected_datetime, model['timestamp'])

    def test_convert_date_field(self):
        model = SpecialDate()
        with self.assertRaises(ValueError):
            model['timestamp'] = 'invalid_input'

    def test_set_date_with_datetime(self):
        model = SpecialDate()
        now = datetime.now()
        model['timestamp'] = now
        self.assertEqual(model['timestamp'], now.date())