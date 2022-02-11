from pyDBMS.database.connections.db_connection import PostgresConnection
from pyDBMS.database.model_descriptor import PostgresDBModelDescriptor, SQLiteModelDescriptor
from pyDBMS.database.abstract_database import AbstractDatabase
from pyDBMS.database.query_builder import PostgresSQLDriver
from pyDBMS.database.type_mapper import PostgresTypeMapper
from pyDBMS.dbtype import DBType, DynamicModel, Model
from typing import Union, List

class PostgresDatabase(AbstractDatabase):
    '''Represents the connection to a sqlite database hosted locally.'''
    
    def __init__(self, **kwargs) -> None:
        super().__init__(PostgresConnection(**kwargs),model_descriptor=PostgresDBModelDescriptor(),sql_driver=PostgresSQLDriver(), type_mapper=PostgresTypeMapper())

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

    def get_model_meta(self, table_name: str) -> DBType:
        q = f'''select column_name, data_type, case when character_maximum_length is not null then character_maximum_length else numeric_precision end as max_length, is_nullable, column_default as default_value from information_schema.columns where table_name = '{table_name}' and table_schema not in ('information_schema', 'pg_catalog') order by table_schema, table_name, ordinal_position;'''
        cur = self.db_connection.cursor()
        cur.execute(q)
        fields = {}
        for row in cur.fetchall():
            col_name = row[0]
            type_string = row[1]
            charlen = row[2]
            nullable = row[3] == 'YES'
            print(col_name)
            fields[col_name] = self.type_mapper.get_type(type_string)(nullable)

        pk_query = f'''SELECT c.column_name, c.data_type
FROM information_schema.table_constraints tc 
JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '{table_name}';
        '''
        cur.execute(pk_query)
        primary_keys = [x[0] for x in cur.fetchall()]
        return DynamicModel(table_name, fields, primary_keys)

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
