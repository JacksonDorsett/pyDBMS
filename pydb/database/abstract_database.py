from abc import ABC, abstractmethod
from typing import List, Union
from pydb.dbtype import Model
from pydb.database.model_descriptor import ModelDescriptor
from pydb.database.connections.db_connection import DBConnection
from pydb.database.query_builder import DeleteQueryBuilder, SelectQueryBuilder, UpdateQueryBuilder

class AbstractDatabase(ABC):
    db_connection = DBConnection
    model_descriptor = ModelDescriptor()

    def __init__(self, model_descriptor : ModelDescriptor, db_connection : DBConnection = None):
        self.model_descriptor = model_descriptor
        self.db_connection = db_connection

    @abstractmethod
    def get_tables(self) -> List[str]:
        pass

    @abstractmethod
    def get_columns(self, table_name : str) -> List[str]:
        pass
    
    @abstractmethod
    def table_exists(self, table_name : str) -> bool:
        pass

    @abstractmethod
    def model_exists(self, model : Model) -> bool:
        if isinstance(model, type):
            model = model()
        table_name = model.__table_name__
        if not self.table_exists(table_name):
            return False
        
        columns = self.get_columns(table_name)
        if all(x in model.fields for x in columns) and len(model.fields) == len(columns):
            return True

        raise ValueError('model fields do not match the database fields')


    @abstractmethod
    def create_model(self, model : Model):
        if isinstance(model,type):
            model = model()
        if self.model_exists(model):
            print(f'model {model.__table_name__} already exists in the database')
            return
        
        self.db_connection.execute(self.model_descriptor.describe(model))
        self.db_connection.commit()

    @abstractmethod
    def insert(self, model : Union[Model, List[Model]]):
        if not isinstance(model, Model):
            raise TypeError()
        
        cur = self.db_connection.cursor()
        fields = sorted(model.fields)
        cur.execute(f'INSERT INTO {model.__table_name__}({",".join(fields)}) VALUES ({",".join(["?" for _ in fields])})',[model.get(field) for field in fields])
        print(f'INSERT INTO {model.__table_name__}({",".join(fields)}) VALUES ({",".join(["?" for _ in fields])})',[model.get(field) for field in fields])
        self.db_connection.commit()

    @abstractmethod
    def delete(self, model_type, override_delete_all = False, **kwargs):
        if isinstance(model_type, type):
            model_type = model_type()

        if len(kwargs) == 0 and not override_delete_all:
            print('Warning: deleting all entries in a table must be explicitly overridden')
            return
        
        query_builder = DeleteQueryBuilder()
        self.db_connection.execute(query_builder.build_query(model_type, **kwargs))
        self.db_connection.commit()

    @abstractmethod
    def update(self, model : Union[Model,List[Model]]) -> int:
        if isinstance(model, list):
            for m in model:
                self.update(m)
            return
        
        if not isinstance(model, Model):
            print(f'{model} is not a subclass of Model')
            return 0
        if not model.__primary_keys__:
            print('model must contain primary keys to be updated')
            
        query_builder = UpdateQueryBuilder()
        query, updatable_fields = query_builder.build_query(model)
        result = self.db_connection.execute(query, list([model.get(x) for x in updatable_fields]))
        self.db_connection.commit()
        return result.rowcount()

    @abstractmethod
    def select(self, model_type : Union[Model,type], **kwargs) -> List[Model]:
        if isinstance(model_type, type):
            model = model_type()
        
        for key in kwargs:
            assert key in model.fields
        
        query_builder = SelectQueryBuilder()
        query = query_builder.build_query(model, **kwargs)

        results = self.db_connection.execute(query)
        return self._build_objects(model_type, results)

    def _build_objects(self, model_type, results):
        fields = results.fields()
        items = []
        for result in results.fetchall():
            obj = model_type()
            for i in range(len(result)):
                obj[fields[i]] = result[i]
            items.append(obj)

        return items