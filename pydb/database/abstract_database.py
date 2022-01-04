from abc import ABC, abstractmethod
from typing import List, Union
from pydb.dbtype import Model
from pydb.database.model_descriptor import ModelDescriptor
from pydb.database.connections.db_connection import DBConnection

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
        pass

    @abstractmethod
    def create_model(self, model : Model):
        pass

    @abstractmethod
    def insert(self, model : Union[Model, List[Model]]):
        pass

    @abstractmethod
    def delete(self, model : Model) -> int:
        pass

    @abstractmethod
    def update(self, model : Union[Model,List[Model]]) -> int:
        pass

    @abstractmethod
    def select(self, model_type : Union[Model,type], **kwargs) -> List[Model]:
        pass