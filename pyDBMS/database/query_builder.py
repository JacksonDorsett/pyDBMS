from abc import ABC, abstractmethod

from numpy import insert
from pyDBMS.dbtype import Model

class QueryBuilder(ABC):
    param_symbol = '?'
    @abstractmethod
    def build_query(self, model : Model, **kwargs) -> str:
        pass

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
        if normal_values:
            field_filters.append(f'{field} in ({",".join(normal_values)})')

        return " OR ".join(field_filters)


class UpdateQueryBuilder(QueryBuilder):
    def build_query(self, model: Model):
        primary_keys = {}
        for x in model.__primary_keys__:
            primary_keys[x] = model[x]

        updatable_fields = set(model.fields) - set(primary_keys)
        return f'UPDATE {model.__table_name__} SET {",".join([x + f"= {self.param_symbol}" for x in updatable_fields])}{self._build_where_clause(primary_keys)}', updatable_fields

class PostgresUpdateQueryBuilder(UpdateQueryBuilder):
    param_symbol = '%s'

class SelectQueryBuilder(QueryBuilder):
    def build_query(self, model : Model, **query_fields):
        return f'SELECT {",".join(model.fields)} FROM {model.__table_name__}{self._build_where_clause(query_fields)}'

class PostgresSelectQueryBuilder(SelectQueryBuilder):
    param_symbol = '%s'

class DeleteQueryBuilder(QueryBuilder):
    def build_query(self, model: Model, **kwargs) -> str:
        return f'DELETE FROM {model.__table_name__}{self._build_where_clause(kwargs)}'

class PostgresDeleteQueryBuilder(DeleteQueryBuilder):
    param_symbol = '%s'

class InsertQueryBuilder(QueryBuilder):
    def build_query(self, model: Model) -> str:
        print([model.get(x) for x in model.fields])
        non_null_fields = [x for x in model.fields if model.get(x) is not None]
        print(f'INSERT INTO {model.__table_name__}({",".join(non_null_fields)}) VALUES ({",".join([self.param_symbol for _ in non_null_fields])})')
        print([model.get(x) for x in non_null_fields])
        # print( f'INSERT INTO {model.__table_name__}({",".join(model.fields)}) VALUES ({",".join([self.param_symbol for _ in model.fields])})')
        return f'INSERT INTO {model.__table_name__}({",".join(model.fields)}) VALUES ({",".join([self.param_symbol for _ in model.fields])})', [model.get(x) for x in model.fields]

class PostgresInsertQueryBuilder(InsertQueryBuilder):
    param_symbol = '%s'

class SQLDriver():
    def __init__(self, update_builder : UpdateQueryBuilder, select_builder : UpdateQueryBuilder, delete_builder : DeleteQueryBuilder, insert_builder : InsertQueryBuilder) -> None:
        self.select_builder = select_builder
        self.update_builder = update_builder
        self.delete_builder = delete_builder
        self.insert_builder = insert_builder

    def build_update(self, model):
        return self.update_builder.build_query(model)

    def build_select(self, model, **query_fields):
        return self.select_builder.build_query(model, **query_fields)

    def build_delete(self, model, **query_fields):
        return self.delete_builder.build_query(model, **query_fields)

    def build_insert(self, model):
        return self.insert_builder.build_query(model)

class StandardSQLDriver(SQLDriver):
    def __init__(self) -> None:
        super().__init__(UpdateQueryBuilder(), SelectQueryBuilder(), DeleteQueryBuilder(), InsertQueryBuilder())

class PostgresSQLDriver(SQLDriver):
    def __init__(self) -> None:
        super().__init__(PostgresUpdateQueryBuilder(), PostgresSelectQueryBuilder(), PostgresDeleteQueryBuilder(), PostgresInsertQueryBuilder())