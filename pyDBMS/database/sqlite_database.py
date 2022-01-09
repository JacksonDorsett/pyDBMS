from pyDBMS.database.connections.db_connection import SQLiteDBConnection
from .abstract_database import AbstractDatabase
from ..dbtype import Model
from typing import Union, List

class SQLiteDatabase(AbstractDatabase):
    '''Represents the connection to a sqlite database hosted locally.'''
    
    def __init__(self, filename, **kwargs) -> None:
        super().__init__(SQLiteDBConnection(filename, **kwargs))

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
