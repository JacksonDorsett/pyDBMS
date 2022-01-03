import unittest
import os
from sqlite3 import connect
from .example_types import SimpleChildModel, SimpleModel
from pydb.database.model_descriptor import SQLiteModelDescriptor
from pydb.database.sqlite_database import SQLiteDatabase

DATABASE_NAME = 'tests/simple_test.db'

class DBTestCase(unittest.TestCase):
    def setUp(self) -> None:
        if os.path.exists(DATABASE_NAME):
            os.remove(DATABASE_NAME)
        self.conn = connect(DATABASE_NAME)
        self.db = SQLiteDatabase(DATABASE_NAME)
        cur = self.conn.cursor()
        cur.execute('''CREATE TABLE simple_model (model_id TEXT PRIMARY KEY,integer_column INTEGER, float_column FLOAT)''')
    
    def tearDown(self) -> None:
        if os.path.exists(DATABASE_NAME):
            os.remove(DATABASE_NAME)
class TestSQLiteDatabase(DBTestCase):

    def test_get_tables(self):
        tables = self.db.get_tables()
        self.assertIn('simple_model', tables)
    
    def test_get_columns(self):
        columns = self.db.get_columns('simple_model')
        self.assertListEqual(sorted(columns), sorted(['model_id','integer_column','float_column']))

    def test_table_exists(self):
        self.assertTrue(self.db.table_exists('simple_model'))
    
    def test_table_not_exists(self):
        self.assertFalse(self.db.table_exists('nonexistant table'))

    def test_model_exists(self):
        self.assertTrue(self.db.model_exists(SimpleModel))

    def test_model_not_exists(self):
        self.assertFalse(self.db.model_exists(SimpleChildModel))

    def test_model_insert(self):
        self.db.create_model(SimpleChildModel())
        self.assertTrue(self.db.model_exists(SimpleChildModel()))

    def test_model_insert_when_exists(self):
        self.db.create_model(SimpleModel())
        self.assertTrue(self.db.model_exists(SimpleModel()))

    def test_model_insert_passing_class(self):
        self.assertFalse(self.db.model_exists(SimpleChildModel))
        self.db.create_model(SimpleChildModel)
        self.assertTrue(self.db.model_exists(SimpleChildModel))

    def test_insert_model(self):
        model = SimpleModel(model_id='test_id',integer_column='100')
        self.db.insert(model)
        results = self.conn.execute(f'select model_id from {model.__table_name__}').fetchall()
        self.assertEqual(1, len(results))
        self.assertEqual(results[0][0], 'test_id')

    def test_select_model_with_single_kwarg(self):
        self._insert_empty_test_model()
        results = self.db.select(SimpleModel, model_id='test_id')
        self.assertEqual(1, len(results))
        self.assertEqual(results[0]['model_id'],'test_id')
    
    def test_select_model_without_kwargs(self):
        self._insert_empty_test_model()
        results = self.db.select(SimpleModel)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['model_id'],'test_id')

    def test_select_model_with_multiple_kwargs(self):
        self._insert_empty_test_model()
        results = self.db.select(SimpleModel, model_id = 'test_id', integer_column=100)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['model_id'],'test_id')

    def test_select_model_with_null(self):
        self._insert_empty_test_model()
        results = self.db.select(SimpleModel, float_column=None)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['model_id'],'test_id')

    def test_select_model_with_null_and_kwarg(self):
        self._insert_empty_test_model()
        self._insert_empty_test_model('test_id2',200,1.0)

        results = self.db.select(SimpleModel, float_column=[None, 1.0])
        returned_ids = sorted([x['model_id'] for x in results])

        self.assertEqual(2, len(results))
        self.assertEqual(['test_id','test_id2'], returned_ids)
    
    def test_select_with_multiple_args_for_field(self):
        self._insert_empty_test_model()
        self._insert_empty_test_model('test_id2',200,1.0)

        results = self.db.select(SimpleModel,model_id=['test_id','test_id2'])
        returned_ids = sorted([x['model_id'] for x in results])

        self.assertEqual(2, len(results))
        self.assertEqual(['test_id','test_id2'], returned_ids)

    def test_delete_without_kwargs(self):
        self._insert_empty_test_model()
        self.db.delete(SimpleModel,False)
        results = self.conn.execute('select * from simple_model').fetchall()
        self.assertEqual(len(results), 1)
        self.db.delete(SimpleModel,True)
        results = self.conn.execute('select * from simple_model').fetchall()
        self.assertEqual(len(results), 0)

    def test_delete_with_single_kwarg(self):
        self._insert_empty_test_model()
        self._insert_empty_test_model('test_id2',200,1.0)
        self.db.delete(SimpleModel, float_column=1.0)
        self.assertEqual(1, len(self.conn.execute('select * from simple_model').fetchall()))
        
    def test_update_simple_model(self):
        self._insert_empty_test_model('test_id2',200,None)
        model = SimpleModel(model_id='test_id2',integer_column=200,float_column=1.0)
        rows_affected = self.db.update(model)
        self.assertEqual(1, rows_affected)
        results = self.conn.execute('select float_column from simple_model').fetchall()
        self.assertEqual(len(results), 1)
        self.assertEqual(1.0, results[0][0])

    def test_invalid_type_for_update(self):
        self.assertEqual(0, self.db.update(100))
        

    def _insert_empty_test_model(self, model_id = 'test_id', integer_column=100, float_column=None):
        self.conn.execute('insert into simple_model(model_id, integer_column, float_column) VALUES (?, ?, ?)', [model_id, integer_column, float_column])
        self.conn.commit()


class TestSQLiteModelDescriptor(DBTestCase):
    def test_describe_model(self):
        descriptor = SQLiteModelDescriptor()
        model = SimpleModel()
        result = descriptor.describe(SimpleModel())

        expected_result = '''CREATE TABLE simple_model (
float_column FLOAT,
integer_column INTEGER,
model_id TEXT,
PRIMARY KEY (model_id)
)'''
        self.assertIsInstance(result, str)
        self.assertEqual(result, expected_result)
