from abc import ABC
from datetime import date, datetime

class DBType(ABC):
    '''Abstract base type for any database objects'''
    _python_type = None

    def __init__(self, is_nullable = True):
        self.is_nullable = is_nullable

    def __str__(self) -> str:
        return self.__class__.__name__.upper()

    def _validate(self, value):
        try:
            converted_value = self._python_type(value)
            return True
        except Exception:
            return False

    def _convert(self, value):
        return self._python_type(value)

class Integer(DBType):
    _python_type = int

class String(DBType):
    _python_type = str
    length = None

    def __str__(self) -> str:
        return 'TEXT'

class Boolean(DBType):
    _python_type = bool

class Float(DBType):
    _python_type = float

class CharN(String):

    def __init__(self, length, is_nullable=True):
        super().__init__(is_nullable=is_nullable)
        self.length = length

    def __str__(self) -> str:
        return f'CHARACTER({self.length})'

    def _convert(self, value):
        return super()._convert(value)[:self.length]

class DateTime(DBType):
    _python_type = datetime

    def _validate(self, value):
        try:
            self._convert(value)
        except:
            return False
        
        return True

    def _convert(self, value):
        if isinstance(value, datetime):
            return value

        if isinstance(value, (float, int)):
            return datetime.fromtimestamp(value)
        
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        
        raise ValueError(f'unexpected value error with {value}')

class Date(DBType):
    _python_type = datetime

    def _validate(self, value):
        try:
            self._convert(value)
        except Exception as e:
            return False
        
        return True

    def _convert(self, value):
        if isinstance(value, datetime):
            return value.date()

        if isinstance(value, date):
            return value

        if isinstance(value, (float, int)):
            if value >= 2 ** 32:
                value /= 1000
            return date.fromtimestamp(value)
        
        if isinstance(value, str):
            return date.fromisoformat(value)
        
        raise ValueError(f'unexpected value error with {value}')


class Model(dict):
    '''
    The model class is the base class for representing a table in a sql database as an object.
    '''
    __table_name__ = None
    __primary_keys__ = []
    
    fields = []

    def __init__(self, **kwargs):
        # init fields
        self.fields, self._type_mapping = self._init_fields()

        #ensure primary keys are valid fields
        if isinstance(self.__primary_keys__, str):
            self.__primary_keys__ = [self.__primary_keys__]
        assert all([x in self.fields for x in self.__primary_keys__])

        for k, v in kwargs.items():
            self[k] = v

    def __setitem__(self, __k, v) -> None:
        if __k not in self.fields:
            raise KeyError(f'{self.__table_name__} does not support field {v}')
        if not self._type_mapping[__k]._validate(v) and v is not None:
            raise ValueError(f'{v} cannot be converted to type `{self._type_mapping[__k].__class__.__name__}`')
        if v is None and not self._type_mapping[__k].is_nullable:
            raise ValueError(f'field {__k} cannot be set to null')
        field_type = getattr(self, __k)
        field_type : DBType
        converted_value = None
        if v is not None:
            converted_value = field_type._convert(v)

        return super().__setitem__(__k, converted_value)



    def _init_fields(self):
        fields = set()
        type_mapping = {}

        for n in dir(self):
            v = getattr(self, n)
            if not callable(v) and isinstance(v, DBType):
                fields.add(n)
                type_mapping[n] = v

        return list(type_mapping.keys()), type_mapping


class DynamicModel(Model):
    def __init__(self,table_name, fields : dict, primary_keys = [], **kwargs):
        if not isinstance(table_name, str) or not table_name:
            raise TypeError('table name must be a string type')

        if not isinstance(primary_keys, (list, str)):
            raise TypeError('primary keys must be a string or list of strings')

        if not table_name or not isinstance(table_name, str):
            raise TypeError('table name must be a string')

        if any([not isinstance(x, str) for x in fields.keys()]):
            raise KeyError('field keys must be strings')

        if any([not isinstance(x, DBType) for x in fields.values()]):
            raise ValueError('values must be a subclass of DBType')

        if isinstance(primary_keys,str):
            primary_keys = [primary_keys]

        self.__primary_keys__ = primary_keys
        self.__table_name__ = table_name
        
        for k, v in fields.items():
            self.__setattr__(k, v)
        super().__init__(**kwargs)

