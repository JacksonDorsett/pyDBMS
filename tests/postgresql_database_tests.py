from datetime import datetime
import unittest
import psycopg2, subprocess
from time import sleep
from multiprocessing import Process
from pyDBMS.database.connections.db_connection import PostgresConnection
from pyDBMS.database.postgres_database import PostgresDatabase
from tests.example_types import LogTimestamp, SimpleChildModel, SimpleModel, SpecialDate

class TestPostgresDB(unittest.TestCase):
    DATABASE_NAME = 'test_database'
    def setUp(self):
        self.conn = psycopg2.connect(host='localhost',dbname='postgres', port='5432', user='postgres', password='#03Threeboys98')
        self.conn.autocommit = True
        self._clear_database(self.conn)
        self.conn = psycopg2.connect(host='localhost',dbname=self.DATABASE_NAME, port='5432', user='postgres', password='#03Threeboys98')
        print('hello')
        self.conn.cursor().execute('''CREATE TABLE simple_model (model_id TEXT PRIMARY KEY,integer_column INTEGER, float_column FLOAT)''')
        self.conn.commit()
        self.db = PostgresDatabase(host='localhost',dbname=self.DATABASE_NAME, port='5432', user='postgres', password='#03Threeboys98')
    

    
    # @classmethod
    # def setUpClass(cls):
    #     cls.database_image_process = Process(target=cls._boot_image,args=(cls,))
    #     cls.database_image_process.start()
    #     sleep(10)
    # @classmethod
    # def tearDownClass(cls):
    #     cls.database_image_process.kill()
    # def _boot_image(cls):
    #     subprocess.run(['bash', '-c', 'docker pull postgres && docker run -d --name postgres -p 5432:4424 -e POSTGRES_USERNAME="postgres" -e POSTGRES_DATABASE="postgres" -e POSTGRES_PASSWORD="password" postgres'])

    def _clear_database(self, connection):
        cur = connection.cursor()
        cur.execute('''SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = 'test_database';''')
        connection.commit()
        cur.execute(f'drop database if exists {self.DATABASE_NAME}')
        cur.execute(f'CREATE DATABASE {self.DATABASE_NAME}')


    def test_get_tables(self):
        tables = self.db.get_tables()
        self.assertIn('simple_model', tables)
    
    def test_get_columns(self):
        columns = self.db.get_columns('simple_model')
        self.assertListEqual(sorted(columns), sorted(['model_id','integer_column','float_column']))

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

    def _insert_empty_test_model(self, model_id = 'test_id', integer_column=100, float_column=None):
        self.conn.cursor().execute('insert into simple_model(model_id, integer_column, float_column) VALUES (%s, %s, %s)', [model_id, integer_column, float_column])
        self.conn.commit()

    def test_create_model(self):
        self.assertFalse(self.db.table_exists('simple_child_model'))
        self.db.create_model(SimpleChildModel())
        self.assertTrue(self.db.table_exists('simple_child_model'))
    
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
