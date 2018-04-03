import json
import queue
from functools import partial
from collections import OrderedDict

from .connections import CURD_FUNCTIONS
from .connections.mysql import MysqlConnection
from .connections.cassandra import CassandraConnection

from .errors import ProgrammingError


class CassandraConnectionPool(CassandraConnection):
    pass


class MysqlConnectionPool(object):
    def __init__(self, *args, **kwargs):
        self.conn_queue = queue.Queue()
        self._args = args
        self._kwargs = kwargs
        
        for func in CURD_FUNCTIONS:
            setattr(self, func, partial(self._wrap_func, func))
            
    def get_connection(self):
        return MysqlConnection(*self._args, **self._kwargs)
        
    def _wrap_func(self, func, *args, **kwargs):
        try:
            conn = self.conn_queue.get_nowait()
        except queue.Empty:
            conn = self.get_connection()
            
        try:
            return getattr(conn, func)(*args, **kwargs)
        except:
            raise
        finally:
            self.conn_queue.put_nowait(conn)
            
    def close(self):
        while True:
            try:
                conn = self.conn_queue.get_nowait()
            except queue.Empty:
                break
            else:
                conn.close()


class Session(object):
    
    '''
    mysql db conf
    {
        'type': 'mysql'
        'conf': {
            'host': '127.0.0.1',
            'port': 3306,
            'user': 'user',
            'password': 'password',
            'max_op_fail_retry': 3,
            'timeout': 60
        }
    }
    tidb conf
    {
        'type': 'mysql'
        'conf': {
            'host': '127.0.0.1',
            'port': 3306,
            'user': 'user',
            'password': 'password',
            'tidb_patch': True,
            'max_op_fail_retry': 3,
            'timeout': 60
        }
    }
    cassandra db conf
    {
        'type': 'cassandra',
        'conf': {
            'hosts': ['127.0.0.1'],
            'username': 'username',
            'password': 'password',
            'max_op_fail_retry': 3,
            'timeout': 60
        }
    }
    '''
    
    def __init__(self, dbs=None):
        self._connection_cache = OrderedDict()
        self._default_connection = None
        
        if dbs:
            for db in dbs:
                self._get_connection(db)
        
    def _create_connection(self, db):
        if db['type'] == 'mysql':
            return MysqlConnectionPool(db['conf'])
        elif db['type'] == 'cassandra':
            return CassandraConnectionPool(db['conf'])
        else:
            raise ProgrammingError('not supported database')
        
    def set_default_connection(self, db):
        key = json.dumps(db)
        conn = self._connection_cache.get(key, None)
        if not conn:
            conn = self._create_connection(db)
            self._connection_cache[key] = conn
        self._default_connection = conn
        
    def _get_connection(self, db):
        key = json.dumps(db)
        conn = self._connection_cache.get(key, None)
        if conn:
            return conn
        else:
            conn = self._create_connection(db)
            self._connection_cache[key] = conn
            
            if not self._default_connection:
                self._default_connection = conn
            
            return conn
        
    def using(self, db=None):
        if db:
            return self._get_connection(db)
        else:
            return self._default_connection
        
    def __getattr__(self, item):
        if item in CURD_FUNCTIONS:
            if self._default_connection:
                return getattr(self._default_connection, item)
            else:
                raise ProgrammingError('no database conf')
        else:
            raise AttributeError
            
    def close(self):
        for k, v in self._connection_cache.items():
            v.close()
        self._connection_cache = OrderedDict()
        self._default_connection = None


class F(object):
    def __init__(self, value):
        self._value = value
    
    def __eq__(self, other):
        return '=', self._value, other
    
    def __ne__(self, other):
        return '!=', self._value, other
    
    def __lt__(self, other):
        return '<', self._value, other
    
    def __le__(self, other):
        return '<=', self._value, other
    
    def __gt__(self, other):
        return '>', self._value, other
    
    def __ge__(self, other):
        return '>=', self._value, other
    
    def __lshift__(self, other):
        return 'IN', self._value, other
    
    
class SimpleCollection(object):
    def __init__(self, session, path, timeout=None, retry=None):
        self.s = session
        self.path = path
        self.timeout = timeout
        self.retry = retry
        
        for func in CURD_FUNCTIONS:
            setattr(
                self,
                func,
                partial(
                    getattr(self.s, func),
                    collection=self.path,
                    timeout=timeout,
                    retry=retry
                )
            )
