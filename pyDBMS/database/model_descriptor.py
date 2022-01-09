from abc import ABC
from pyDBMS.dbtype import CharN, DBType, Float, Integer, Model, String

class StandardModelDescriptor(ABC):
    '''Responsible for describing the models meta information'''

    supported_types = {
        Integer : 'INTEGER',
        Float : 'FLOAT',
        String : 'TEXT',
        CharN : 'TEXT'
    }

    def describe(self, model : Model):
        '''
        Describes the sql command required to created to insert
        The model into the table.
        '''

        columns = []
        for field in sorted(model.fields):
            if type(getattr(model, field)) not in self.supported_types:
                raise NotImplementedError()
            c = field + ' '
            type_object = model._type_mapping[field]
            type_str = str(type_object)
            
            c += type_str
            if not type_object.is_nullable:
                c += ' NOT NULL'
            columns.append(c)
        
        if model.__primary_keys__:
            columns.append(f'PRIMARY KEY ({",".join(model.__primary_keys__)})')
        inner_str = ",\n".join(columns)
        query = 'CREATE TABLE ' + model.__table_name__ + ' (\n' + inner_str +'\n)'
        return query

class SQLiteModelDescriptor(StandardModelDescriptor):
    supported_types = {
        Integer : 'INTEGER',
        Float : 'FLOAT',
        String : 'TEXT',
        CharN : 'TEXT'
    }

    
class CrateDBModelDescriptor(SQLiteModelDescriptor):
    type_mapping = {
        Integer : 'INTEGER',
        Float : 'FLOAT',
        String : 'TEXT',
        CharN : 'TEXT'
    }
