from datetime import datetime
from time import sleep
import subprocess, os, unittest
from multiprocessing import Process
from typing import List, Union
from sqlite3 import connect
from crate import client
from pyDBMS.database.abstract_database import AbstractDatabase
from pyDBMS.database.crate_database import CrateDatabase
from .example_types import CharNModel, LogTimestamp, NoPrimaryKeyModel, NonNullableModel, SimpleChildModel, SimpleModel, SimpleTextModel, SpecialDate
from pyDBMS.database.sqlite_database import SQLiteDatabase
from pyDBMS.database.connections.db_connection import SQLiteDBConnection, SQLiteDBCursor
from pyDBMS.dbtype import Model
DATABASE_NAME = 'tests/simple_test.db'

class TestAbstractDB(unittest.TestCase):
    class MockDB(AbstractDatabase):
        def __init__(self):
            super().__init__(None, None)

        def get_tables(self) -> List[str]:
            return super().get_tables()

        def get_columns(self, table_name: str) -> List[str]:
            return super().get_columns(table_name)
        
        def table_exists(self, table_name):
            return super().table_exists(table_name)

        def model_exists(self, model: Model) -> bool:
            return super().model_exists(model)

        def create_model(self, model):
            return super().create_model(model)
            
        def insert(self, model):
            return super().insert(model)

        def delete(self, model_type, override_delete_all=False, **kwargs):
            return super().delete(model_type, override_delete_all=override_delete_all, **kwargs)
            
        def select(self, model_type: Union[Model, type], **kwargs) -> List[Model]:
            return super().select(model_type, **kwargs)

        def update(self, model: Union[Model, List[Model]]) -> int:
            return super().update(model)

    def test_calling_get_tables_when_not_implemented(self):
        db = self.MockDB()
        with self.assertRaises(NotImplementedError):
            db.get_tables()

    def test_calling_get_columns_when_not_implemented(self):
        db = self.MockDB()
        with self.assertRaises(NotImplementedError):
            db.get_columns('table')
        

class SQLiteDBTestCase(unittest.TestCase):
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
class TestSQLiteDatabase(SQLiteDBTestCase):

    def test_get_tables(self):
        tables = self.db.get_tables()
        self.assertIn('simple_model', tables)
    
    def test_get_columns(self):
        columns = self.db.get_columns('simple_model')
        self.assertListEqual(sorted(columns), sorted(['model_id','integer_column','float_column']))

    def test_get_columns_from_invalid_table(self):
        with self.assertRaises(KeyError):
            self.db.get_columns('invalid_table')

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

    def test_model_insert_with_mismatch_columns(self):
        self.conn.execute('CREATE TABLE non_nullable_model (model_id TEXT PRIMARY KEY, other_column INTEGER)')
        self.conn.commit()
        with self.assertRaises(ValueError):
            self.db.model_exists(NonNullableModel())    
        pass

    def test_insert_model_with_invalid_input_type(self):
        with self.assertRaises(TypeError):
            self.db.insert(SimpleModel)

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

    def test_update_on_primary_keys(self):
        self.db.create_model(NoPrimaryKeyModel)
        model = NoPrimaryKeyModel(model_id='test_id')
        self.assertEqual(self.db.update(model), 0)

    def test_update_multiple(self):
        self._insert_empty_test_model()
        self._insert_empty_test_model('test_id2')
        self.db.update([SimpleModel(model_id ='test_id',integer_column=200),SimpleModel(model_id ='test_id2',integer_column=200)])
        results = self.conn.execute('select integer_column from simple_model').fetchall()
        self.assertEqual(2,len(results))
        for result in results:
            self.assertEqual(200,result[0])
        
    def test_insert_charn_model_and_insert(self):
        self.db.create_model(CharNModel)
        self.assertIn('charn_model',self.db.get_tables())
        model = CharNModel()
        model['model_id'] = '012345678910'
        self.db.insert(model)
        cur = self.conn.execute('select model_id from charn_model')
        self.assertEqual('0123456789', cur.fetchone()[0])

    def test_insert_select_bool_type(self):
        self.db.create_model(SimpleTextModel)
        model = SimpleTextModel(model_id='12345', boolean_column=True)
        self.db.insert(model)
        results = self.db.select(SimpleTextModel)
        self.assertEqual(1,len(results))
        results = results[0]
        self.assertIsInstance(results['boolean_column'], bool)
        self.assertEqual(results['boolean_column'], True)

    def test_insert_select_charn_type_as_integer(self):
        self.db.create_model(SimpleTextModel)
        model = SimpleTextModel(model_id=1234567890)
        self.db.insert(model)
        results = self.db.select(SimpleTextModel)
        self.assertEqual(1,len(results))
        results = results[0]
        self.assertIsInstance(results['model_id'], str)
        self.assertEqual('12345', results['model_id'])
    
    def test_insert_and_select_date(self):
        self.db.create_model(SpecialDate)
        model = SpecialDate(timestamp=datetime.now().today(), model_id = 'test_id')
        self.db.insert(model)
        result = self.db.select(SpecialDate)
        self.assertEqual(1, len(result))
        result = result[0]
        self.assertEqual(model['timestamp'], result['timestamp'])

    def test_insert_and_select_datetime(self):
        self.db.create_model(LogTimestamp)
        model = LogTimestamp(timestamp=datetime.now(), model_id = 'test_id')
        self.db.insert(model)

        result = self.db.select(LogTimestamp)
        self.assertEqual(1, len(result))
        result = result[0]
        self.assertEqual(model['timestamp'], result['timestamp'])


    def test_invalid_type_for_update(self):
        self.assertEqual(0, self.db.update(100))
        
    def _insert_empty_test_model(self, model_id = 'test_id', integer_column=100, float_column=None):
        self.conn.execute('insert into simple_model(model_id, integer_column, float_column) VALUES (?, ?, ?)', [model_id, integer_column, float_column])
        self.conn.commit()

class TestSQLiteConnection(SQLiteDBTestCase):
    def test_fields_method(self):
        connection = SQLiteDBConnection(DATABASE_NAME)
        fields = ['model_id', 'float_column', 'integer_column']
        query = f'select {",".join(fields)} from simple_model'
        result = connection.execute(query)
        self.assertEqual(result.fields(), fields)

class CrateDBTestCase(unittest.TestCase):
    def setUp(self):
        self.conn = client.connect('localhost:4200', username='crate')
        self._clear_database(self.conn)
        self.conn.cursor().execute('''CREATE TABLE simple_model (model_id TEXT PRIMARY KEY,integer_column INTEGER, float_column FLOAT)''')
        self.db = CrateDatabase(servers='localhost:4200', username='crate')
    def tearDown(self) -> None:
        self.conn.cursor().execute('drop table if exists simple_model')
        
    
    @classmethod
    def setUpClass(cls):
        cls.database_image_process = Process(target=cls._boot_image,args=(cls,))
        cls.database_image_process.start()
        sleep(10)
    @classmethod
    def tearDownClass(cls):
        cls.database_image_process.kill()
    def _boot_image(cls):
        subprocess.run(['bash', '-c', 'docker pull crate && docker run --publish 4200:4200 --publish 5431:5431 crate -Cdiscovery.type=single-node'])

    def _clear_database(self, connection):
        cur = connection.cursor()
        cur.execute('SHOW TABLES')
        for table in cur.fetchall():
            table = table[0]
            print(f"deleting table {table}")

            cur.execute(f'drop table if exists {table}')
        pass

class TestCrateDB(CrateDBTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
    
    def test_local_connection(self):
        cur = self.conn.cursor()
        cur.execute('SHOW COLUMNS FROM simple_model')
        self.assertEqual(sorted(['model_id','integer_column','float_column']),sorted([x[0] for x in cur.fetchall()]))

    def test_get_tables(self):
        self.assertEqual(['simple_model'],self.db.get_tables())

    def test_get_columns(self):
        self.assertEqual(sorted(['model_id','integer_column','float_column']), sorted(self.db.get_columns('simple_model')))

    def test_get_columns_for_nonexistant_table(self):
        with self.assertRaises(KeyError):
            self.db.get_columns('invalid_table')

    def test_table_exists(self):
        self.assertTrue(self.db.table_exists('simple_model'))

    def test_table_not_exists(self):
        self.assertFalse(self.db.table_exists('invalid_model'))

    def test_model_exists(self):
        self.assertTrue(self.db.model_exists(SimpleModel))
    
    def test_model_not_exists(self):
        self.assertFalse(self.db.model_exists(SimpleChildModel))


    def test_create_model(self):
        self.assertFalse(self.db.table_exists('simple_child_model'))
        self.db.create_model(SimpleChildModel())
        self.assertTrue(self.db.table_exists('simple_child_model'))

    def test_insert_model(self):
        model = SimpleModel(model_id='test_id',integer_column=100)
        self.db.insert(model)
        sleep(1)
        cur = self.conn.cursor()
        cur.execute('select model_id, integer_column, float_column from simple_model')
        result = cur.fetchone()
  
        self.assertEqual(result[0], 'test_id')
        
    
    def test_model_insert_passing_class(self):
        self.assertFalse(self.db.model_exists(SimpleChildModel))
        self.db.create_model(SimpleChildModel)
        self.assertTrue(self.db.model_exists(SimpleChildModel))

    def test_select_model_with_single_kwarg(self):
        self._insert_empty_test_model()
        
        results = self.db.select(SimpleModel, model_id='test_id')
        self.assertEqual(1, len(results))
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

    def test_update_simple_model(self):
        self._insert_empty_test_model('test_id2',200,None)
        model = SimpleModel(model_id='test_id2',integer_column=200,float_column=1.0)
        rows_affected = self.db.update(model)
        self.assertEqual(1, rows_affected)
        sleep(2)
        cur = self.conn.cursor()
        cur.execute('select float_column from simple_model')
        results = cur.fetchall()
        self.assertEqual(len(results), 1)
        self.assertEqual(1.0, results[0][0])


    def test_delete_without_kwargs(self):
        self._insert_empty_test_model()
        cur = self.conn.cursor()
        self.db.delete(SimpleModel,False)
        sleep(1)
        cur.execute('select * from simple_model')
        results = cur.fetchall()
        self.assertEqual(len(results), 1)
        self.db.delete(SimpleModel,True)
        sleep(1)
        cur.execute('select * from simple_model')
        results = cur.fetchall()
        self.assertEqual(len(results), 0)
    def test_delete_with_single_kwarg(self):
        self._insert_empty_test_model()
        self._insert_empty_test_model('test_id2',200,1.0)
        cur = self.conn.cursor()
        self.db.delete(SimpleModel, float_column=1.0)
        sleep(4)
        cur.execute('select * from simple_model')
        self.assertEqual(1, len(cur.fetchall()))

    def test_insert_and_select_date(self):
        self.db.create_model(SpecialDate)
        model = SpecialDate()
        model['timestamp'] = datetime.now()
        model['model_id'] = 'test_id'
        self.db.insert(model)
        sleep(2)
        result = self.db.select(SpecialDate)
        self.assertEqual(1, len(result))
        result = result[0]
        self.assertEqual(model['timestamp'], result['timestamp'])

    def test_insert_and_select_datetime(self):
        self.db.create_model(LogTimestamp)
        model = LogTimestamp()
        model['timestamp'] = datetime.now()
        model['model_id'] = 'test_id'
        self.db.insert(model)
        sleep(2)
        result = self.db.select(LogTimestamp)
        self.assertEqual(1, len(result))
        result = result[0]
        self.assertEqual(model['timestamp'], result['timestamp'])
        
    def test_insert_table_with_timestamp(self):
        self.assertNotIn('log_timestamp_model', self.db.get_tables())
        self.db.create_model(LogTimestamp)
        self.assertIn('log_timestamp_model', self.db.get_tables())
        pass
    def _insert_empty_test_model(self, model_id = 'test_id', integer_column=100, float_column=None):
        self.conn.cursor().execute('insert into simple_model(model_id, integer_column, float_column) VALUES (?, ?, ?)', [model_id, integer_column, float_column])
        self.conn.commit()
        sleep(2)

