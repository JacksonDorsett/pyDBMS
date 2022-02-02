from sqlalchemy import sql
from pyDBMS.database.connections.db_connection import PostgresConnection
from pyDBMS.database.model_descriptor import PostgresDBModelDescriptor, SQLiteModelDescriptor
from pyDBMS.database.abstract_database import AbstractDatabase
from pyDBMS.database.query_builder import PostgresSQLDriver
from pyDBMS.dbtype import Model
from typing import Union, List

class PostgresDatabase(AbstractDatabase):
    '''Represents the connection to a sqlite database hosted locally.'''
    
    def __init__(self, **kwargs) -> None:
        super().__init__(PostgresConnection(**kwargs),model_descriptor=PostgresDBModelDescriptor(),sql_driver=PostgresSQLDriver())

    def get_tables(self):
        cur = self.db_connection.cursor()
        cur.execute("""SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE';""")
        return [x[0] for x in cur.fetchall()]

    def get_columns(self, table_name):
        if table_name not in self.get_tables():
            raise KeyError()
        cur = self.db_connection.cursor()
        data = cur.execute(f'''SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';''')
        return [x[0] for x in data.fetchall()]

    def table_exists(self, table_name):
        return super().table_exists(table_name)

    def model_exists(self, model: Model) -> bool:
        return super().model_exists(model)

    def create_model(self, model):
        return super().create_model(model)
        
    def insert(self, model : Model):
        cur = self.db_connection.cursor()
        cur.execute(f'INSERT INTO {model.__table_name__} ({",".join(model.fields)}) VALUES ({",".join(["%s" for x in model.fields])})',[model.get(field) for field in model.fields])
        self.db_connection.commit()

    def delete(self, model_type, override_delete_all=False, **kwargs):
        return super().delete(model_type, override_delete_all=override_delete_all, **kwargs)
        
    def select(self, model_type: Union[Model, type], **kwargs) -> List[Model]:
        return super().select(model_type, **kwargs)

    def update(self, model: Union[Model, List[Model]]) -> int:
        return super().update(model)
