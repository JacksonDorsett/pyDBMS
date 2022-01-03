from abc import ABC
from pydb.dbtype import DBType, Float, Integer, Model, Text

class ModelDescriptor(ABC):
    '''Responsible for describing the models meta information'''
    def describe(self, model : Model):
        '''
        Describes the sql command required to created to insert
        The model into the table.
        '''
        pass

class SQLiteModelDescriptor(ModelDescriptor):
    type_mapping = {
        Integer : 'INTEGER',
        Float : 'FLOAT',
        Text : 'TEXT'
    }
    def describe(self, model : Model):
        columns = []
        for field in sorted(model.fields):
            c = field + ' '
            type_object = model._type_mapping[field]
            type_str = self.type_mapping.get(type(type_object))
            if not type_str:
                raise NotImplementedError()
            c += type_str
            if not type_object.is_nullable:
                c += ' NOT NULL'
            columns.append(c)
        
        if model.__primary_keys__:
            columns.append(f'PRIMARY KEY ({",".join(model.__primary_keys__)})')
        inner_str = ",\n".join(columns)
        query = 'CREATE TABLE ' + model.__table_name__ + ' (\n' + inner_str +'\n)'
        return query
        