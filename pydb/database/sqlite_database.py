import sqlite3

from pydb.database.model_descriptor import SQLiteModelDescriptor
from .abstract_database import AbstractDatabase
from ..dbtype import Model, Text, Float, Integer

class SQLiteDatabase(AbstractDatabase):
    '''Represents the connection to a sqlite database hosted locally.'''
    type_mapping = {
        'TEXT' : Text,
        'INTEGER' : Integer,
        'FLOAT' : Float
    }
    def __init__(self, filename, db_exists = False) -> None:
        super().__init__(SQLiteModelDescriptor())
        self.filename = filename
        self.db_exists = db_exists
        self.db_connection = sqlite3.connect(self.filename)
    

    def get_tables(self):
        cur = self.db_connection.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [x[0] for x in cur.fetchall()]

    
    def get_columns(self, table_name):
        if table_name not in self.get_tables():
            raise KeyError()
        cur = self.db_connection.cursor()
        data = cur.execute(f'''PRAGMA table_info({table_name});''')
        return [x[1] for x in data.fetchall()]

    def table_exists(self, table_name):
        return table_name in self.get_tables()

    def model_exists(self, model : Model):
        if isinstance(model, type):
            model = model()
        table_name = model.__table_name__
        if not self.table_exists(table_name):
            return False
        
        columns = self.get_columns(table_name)
        if all(x in model.fields for x in columns) and len(model.fields) == len(columns):
            return True

        raise ValueError('model fields do not match the database fields')

    def create_model(self, model):
        if isinstance(model,type):
            model = model()
        if self.model_exists(model):
            print(f'model {model.__table_name__} already exists in the database')
            return
        
        self.db_connection.execute(self.model_descriptor.describe(model))

        self.db_connection.commit()
        
    def insert(self, model):
        if not isinstance(model, Model):
            raise TypeError()
        
        cur = self.db_connection.cursor()
        fields = sorted(model.fields)
        cur.execute(f'INSERT INTO {model.__table_name__}({",".join(fields)}) VALUES ({",".join(["?" for _ in fields])})',[model.get(field) for field in fields])
        cur.connection.commit()

    def delete(self, model_type, override_delete_all = False, **kwargs):
        if isinstance(model_type, type):
            model_type = model_type()

        if len(kwargs) == 0 and not override_delete_all:
            print('Warning: deleting all entries in a table must be explicitly overridden')
            return
        
        query = f'DELETE FROM {model_type.__table_name__}{self._build_where_clause(kwargs)}'
        self.db_connection.execute(query)
        self.db_connection.commit()
    
    def select(self, model_type, **kwargs):
        if isinstance(model_type, type):
            model = model_type()
        
        for key in kwargs:
            assert key in model.fields
        
        fields = model.fields
        query = f'SELECT {",".join(fields)} FROM {model.__table_name__}{self._build_where_clause(kwargs)}'

        results = self.db_connection.execute(query).fetchall()
        return self._build_objects(model_type, fields, results)

    def update(self, model):
        if isinstance(model, list):
            for m in model:
                self.update(m)
            return
        
        if not isinstance(model, Model):
            print(f'{model} is not a subclass of Model')
            return 0
        if not model.__primary_keys__:
            print('model must contain primary keys to be updated')

        primary_keys = {}
        for x in model.__primary_keys__:
            primary_keys[x] = model[x]

        query = f'UPDATE {model.__table_name__} SET {",".join([x + "= ?" for x in model.keys()])}{self._build_where_clause(primary_keys)}'
        result = self.db_connection.execute(query, list(model.values()))
        self.db_connection.commit()
        return result.rowcount

    def _build_objects(self, model_type, fields, results):
        items = []
        for result in results:
            obj = model_type()
            for i in range(len(result)):
                obj[fields[i]] = result[i]
            items.append(obj)

        return items

    def _build_where_clause(self, kwargs):
        if len(kwargs) == 0:
            return ''
        kwargs = self._normalize_fields(**kwargs)
        
        field_filter_strs = []
        for field, value in kwargs.items():
            field_filter_strs.append(self._process_field(field,value))

        return f' WHERE {" AND ".join(field_filter_strs)}'

    def _normalize_fields(self, **kwargs):
        fields = {}
        for k,v in kwargs.items():
            if not isinstance(v, list):
                v = [v]
            fields[k] = v
        return fields

    def _process_field(self, field, values):
        field_filters = []
        if None in values:
            field_filters.append(f'{field} is null')
            values = [v for v in values if v is not None]
        
        normal_values = []
        for value in values:
            if isinstance(value, str):
                normal_values.append("'" + str(value) + "'")
            else:
                normal_values.append(str(value))
        field_filters.append(f'{field} in ({",".join(normal_values)})')

        return " OR ".join(field_filters)