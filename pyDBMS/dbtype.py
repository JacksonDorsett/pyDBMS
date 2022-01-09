from abc import ABC

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

        if isinstance(field_type, String) and field_type.length:
            v = v[:field_type.length]
        return super().__setitem__(__k, v)



    def _init_fields(self):
        fields = set()
        type_mapping = {}

        for n in dir(self):
            v = getattr(self, n)
            if not callable(v) and isinstance(v, DBType):
                fields.add(n)
                type_mapping[n] = v

        return list(type_mapping.keys()), type_mapping




