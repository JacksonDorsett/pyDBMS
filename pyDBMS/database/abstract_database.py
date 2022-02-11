from abc import ABC, abstractmethod
from typing import List, Union
from pyDBMS.database.type_mapper import TypeMapper
from pyDBMS.dbtype import DBType, Model
from pyDBMS.database.model_descriptor import StandardModelDescriptor
from pyDBMS.database.connections.db_connection import DBConnection
from pyDBMS.database.query_builder import DeleteQueryBuilder, SQLDriver, SelectQueryBuilder, StandardSQLDriver, UpdateQueryBuilder

class AbstractDatabase(ABC):
    db_connection : DBConnection
    sql_driver : SQLDriver
    model_descriptor = StandardModelDescriptor()
    
    def __init__(self,db_connection : DBConnection, model_descriptor = StandardModelDescriptor(), sql_driver = StandardSQLDriver(), type_mapper = TypeMapper()):
        self.model_descriptor = model_descriptor
        self.db_connection = db_connection
        self.sql_driver = sql_driver
        self.type_mapper = type_mapper

    @abstractmethod
    def get_tables(self) -> List[str]:
        raise NotImplementedError()

    @abstractmethod
    def get_columns(self, table_name : str) -> List[str]:
        raise NotImplementedError()

    @abstractmethod
    def get_model_meta(self, table_name : str, column_name : str) -> DBType:
        raise NotImplementedError()
    
    @abstractmethod
    def table_exists(self, table_name : str) -> bool:
        return table_name in self.get_tables()

    @abstractmethod
    def model_exists(self, model : Model) -> bool:
        if isinstance(model, type):
            model = model()
        table_name = model.__table_name__
        if not self.table_exists(table_name):
            return False
        
        columns = self.get_columns(table_name)
        if sorted(model.fields) == sorted(columns):
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
        self.db_connection.commit()

    @abstractmethod
    def delete(self, model_type, override_delete_all = False, **kwargs):
        if isinstance(model_type, type):
            model_type = model_type()

        if len(kwargs) == 0 and not override_delete_all:
            print('Warning: deleting all entries in a table must be explicitly overridden')
            return

        self.db_connection.execute(self.sql_driver.build_delete(model_type, **kwargs))
        self.db_connection.commit()

    @abstractmethod
    def update(self, model : Union[Model,List[Model]]) -> int:
        if isinstance(model, list):
            affected_rows = 0
            for m in model:
                affected_rows += self.update(m)
            return affected_rows
        
        if not isinstance(model, Model):
            print(f'{model} is not a subclass of Model')
            return 0
        if not model.__primary_keys__:
            print('model must contain primary keys to be updated')
            return 0

        query, updatable_fields = self.sql_driver.build_update(model)
        result = self.db_connection.execute(query, list([model.get(x) for x in updatable_fields]))
        self.db_connection.commit()
        return result.rowcount()

    @abstractmethod
    def select(self, model_type : Union[Model,type], **kwargs) -> List[Model]:
        if isinstance(model_type, type):
            model = model_type()
        else:
            model = model_type
        
        for key in kwargs:
            assert key in model.fields

        query = self.sql_driver.build_select(model, **kwargs)

        results = self.db_connection.execute(query)
        return self._build_objects(model_type, results)

    def _build_objects(self, model_type, results):
        fields = results.fields()
        items = []
        for result in results.fetchall():
            
            obj = model_type() if isinstance(model_type, type) else model_type
            for i in range(len(result)):
                obj[fields[i]] = result[i]
            items.append(obj)

        return items