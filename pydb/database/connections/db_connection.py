from abc import ABC, abstractmethod
import sqlite3
from sqlite3.dbapi2 import Connection, Cursor
class DBCursor(ABC):
    _cursor_impl : Cursor
    def __init__(self, cursor_impl) -> None:
        self._cursor_impl = cursor_impl

    @abstractmethod
    def execute(self, sql, params = None):
        pass

    @abstractmethod
    def fetchall(self):
        pass

    @abstractmethod
    def fetchone(self):
        pass

    @abstractmethod
    def fetchmany(self,n):
        pass

    @abstractmethod
    def rowcount(self):
        pass
    pass

class DBConnection(ABC):
    _connection_impl : Connection
    def __init__(self, **connection_args) -> None:
        super().__init__()

    @abstractmethod
    def cursor(self):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def execute(self, sql, params):
        pass

class SQLiteDBCursor(DBCursor):
    def execute(self, sql, params = None):
        if params:
            return SQLiteDBCursor(self._cursor_impl.execute(sql, params))
        else:
            return SQLiteDBCursor(self._cursor_impl.execute(sql))

    def fetchall(self):
        return self._cursor_impl.fetchall()

    def fetchone(self):
        return self._cursor_impl.fetchone()

    def fetchmany(self,n):
        return self._cursor_impl.fetchmany(n)

    def rowcount(self):
        return self._cursor_impl.rowcount

    def fields(self):
        return [x[0] for x in self._cursor_impl.description]

class SQLiteDBConnection(DBConnection):
    _connection_impl : sqlite3.Connection
    def __init__(self, filename, **connection_args) -> None:
        self._connection_impl = sqlite3.connect(filename, **connection_args)
    
    def cursor(self):
        return SQLiteDBCursor(self._connection_impl.cursor())

    def commit(self):
        self._connection_impl.commit()

    def execute(self, sql, params = None):
        if params:
            return SQLiteDBCursor(self._connection_impl.execute(sql, params))
        else:
            return SQLiteDBCursor(self._connection_impl.execute(sql))

from crate.client import connect, connection

class CrateDBCursor(DBCursor):
    _connection_impl : connection.Connection
    def execute(self, sql, params = None):
        if params:
            self._cursor_impl.execute(sql, params)
        else:
            self._cursor_impl.execute(sql)
        return self

    def fetchall(self):
        return self._cursor_impl.fetchall()

    def fetchone(self):
        return self._cursor_impl.fetchone()

    def fetchmany(self,n):
        return self._cursor_impl.fetchmany(n)

    def rowcount(self):
        return self._cursor_impl.rowcount

    def fields(self):
        return [x[0] for x in self._cursor_impl.description]

class CrateDBConnection(DBConnection):
    _connection_impl : connection.Connection

    def __init__(self, servers, **connection_args) -> None:
        super().__init__(**connection_args)
        self._connection_impl = connect(servers, **connection_args)

    def cursor(self):
        return CrateDBCursor(self._connection_impl.cursor())

    def commit(self):
        self._connection_impl.commit()

    def execute(self, sql, params = None):
        cur = self._connection_impl.cursor()
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        return CrateDBCursor(cur)



