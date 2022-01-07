from pydb.database.abstract_database import AbstractDatabase
from pydb.database.connections.db_connection import CrateDBConnection
from typing import List, Union
from pydb.dbtype import Model

class CrateDatabase(AbstractDatabase):
    def __init__(self, servers, **connection_args):
        super().__init__(db_connection=CrateDBConnection(servers,**connection_args))

    def get_tables(self) -> List[str]:
        cur = self.db_connection.cursor()
        cur.execute('SHOW TABLES')
        return [x[0] for x in cur.fetchall()]

    def get_columns(self, table_name : str) -> List[str]:
        if table_name not in self.get_tables():
            raise KeyError()
        cur = self.db_connection.cursor()
        cur.execute(f'SHOW COLUMNS FROM {table_name}')
        return [x[0] for x in cur.fetchall()]

    def table_exists(self, table_name : str) -> bool:
        return table_name in self.get_tables()

    def model_exists(self, model : Model) -> bool:
        return super().model_exists(model)

    def create_model(self, model : Model):
        return super().create_model(model)

    def insert(self, model : Union[Model, List[Model]]):
        return super().insert(model)

    def delete(self, model_type, override_delete_all = False, **kwargs):
        return super().delete(model_type,override_delete_all, **kwargs)    

    def update(self, model: Union[Model, List[Model]]) -> int:
        return super().update(model)

    def select(self, model_type: Union[Model, type], **kwargs) -> List[Model]:
        return super().select(model_type, **kwargs)
