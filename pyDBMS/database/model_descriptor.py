from abc import ABC
from pyDBMS.dbtype import Boolean, CharN, DBType, Date, DateTime, Float, Integer, Model, String
DEFAULT = object()
class StandardModelDescriptor(ABC):
    '''Responsible for describing the models meta information'''


    supported_types = {
        Integer : DEFAULT,
        Float : DEFAULT,
        String : DEFAULT,
        CharN : DEFAULT,
        Boolean : DEFAULT
    }

    special_type_conversions = {

    }

    def __init__(self) -> None:
        self.supported_types.update(type(self).supported_types)
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
            type_str = str(type_object) if self.supported_types[type(type_object)] == DEFAULT else self.supported_types[type(type_object)]
            
            c += type_str
            if not type_object.is_nullable:
                c += ' NOT NULL'
            columns.append(c)
        
        if model.__primary_keys__:
            columns.append(f'PRIMARY KEY ({",".join(model.__primary_keys__)})')
        inner_str = ",\n".join(columns)
        query = 'CREATE TABLE ' + model.__table_name__ + ' (\n' + inner_str +'\n)'
        return query

    def normalize_types(self, model : Model) -> dict:
        normalized_values = {}
        for k, v in model:
            value_type = type(getattr(model, k))
            if value_type in self.special_type_conversions:
                normalized_values[k] = self.special_type_conversions[value_type](v)
            else:
                normalized_values[k] = v

        return normalized_values

class SQLiteModelDescriptor(StandardModelDescriptor):
    supported_types = {
        Integer : DEFAULT,
        Float : DEFAULT,
        String : DEFAULT,
        Boolean : DEFAULT,
        CharN : DEFAULT,
        DateTime : DEFAULT,
        Date : DEFAULT
    }

    
class CrateDBModelDescriptor(StandardModelDescriptor):
    
    supported_types = {
        Integer : DEFAULT,
        Float : DEFAULT,
        String : DEFAULT,
        Boolean : DEFAULT,
        CharN : DEFAULT,
        DateTime : 'TIMESTAMP',
        Date : 'TIMESTAMP'
    }
    def __init__(self) -> None:
        special_type_conversions = {
        Date : self._convert_date
        }
        super().__init__()

    def _convert_date(self, value):
        pass


class PostgresDBModelDescriptor(StandardModelDescriptor):
    supported_types = {
        Integer : DEFAULT,
        Float : DEFAULT,
        String : DEFAULT,
        Boolean : DEFAULT,
        CharN : DEFAULT,
        DateTime : 'TIMESTAMP',
        Date : DEFAULT
    }