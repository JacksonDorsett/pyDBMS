import unittest
from ..pydb import type

class TestDatabaseTypes(unittest.TestCase):
    pass
    # valid_inits = {
    #     type.Integer : [10, '100', 0, 1.1, type.Text('1000')],
    #     type.Text : ['100', 10, 10.2, '1.', 1.],
    #     type.Float : ['1.1', 100, 1.1, 0, 0.1, 1.] 
    # }

    # invalid_inits = {
    #     type.Integer : ['ab', '', None, '0a'],
    #     type.Float : ['a', '1.a', '', None]
    # }

    # def test_valid_type_init(self):
    #     for type, values in self.valid_inits.items():
    #         for value in values:
    #             db_obj = type(value)
    #             self.assertIsInstance(db_obj._value, type._python_type)


    # def test_invalid_type_init(self):
    #     for type, values in self.invalid_inits.items():
    #         for value in values:
    #             with self.assertRaises((TypeError, ValueError)):
    #                 type(value)

