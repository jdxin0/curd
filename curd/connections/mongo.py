import pymongo

from ..errors import (
    UnexpectedError, OperationFailure, ProgrammingError,
    ConnectError,
    DuplicateKeyError
)
from . import logger


class MongoConnection(object):
    def __init__(self, conf):
        self._conf = conf
        self.client = None

        self._cache_dbs = {}
        self._cache_collections = {}

    def _connect(self, conf):
        client = pymongo.MongoClient(**conf)
        return client

    def connect(self, conf):
        try:
            return self._connect(conf)
        except Exception as e:
            raise ConnectError(origin_error=e)
        
    def close(self):
        try:
            self.client.close()
        except Exception as e:
            logger.warning(str(e))
            
        self.client = None
        
    def _get_db(self, db_name):
        db = self._cache_dbs.get(db_name, None)
        if db:
            return db
        else:
            db = getattr(self.client, db_name)
            self._cache_dbs[db_name] = db
            return db
    
    def _get_collection(self, db_name, collection_name):
        key = db_name + '.' + collection_name
        collection = self._cache_collections.get(key, None)
        if collection:
            return collection
        else:
            collection = getattr(self._get_db(db_name), collection_name)
            self._cache_collections[collection_name] = collection
            return collection
        
    def _execute(self, db, collection, operation, *args, **kwargs):
        if not self.client:
            self.client = self.connect(self._conf)
    
        collection = self._get_collection(db, collection)
        func = getattr(collection, operation)
    
        try:
            return func(*args, **kwargs)
        except pymongo.errors.OperationFailure as e:
            raise OperationFailure(origin_error=e)
        except (
                pymongo.errors.DuplicateKeyError,
                pymongo.errors.InvalidOperation,
                pymongo.errors.InvalidName,
                pymongo.errors.CollectionInvalid,
                pymongo.errors.DocumentTooLarge,
        ) as e:
            raise ProgrammingError(origin_error=e)
        except Exception as e:
            raise UnexpectedError(origin_error=e)
        
    def execute(self, db, collection, operation, *args, **kwargs):
        try:
            return list(self._execute(db, collection, operation, *args, **kwargs))
        except OperationFailure:
            self.close()
            raise
        except ProgrammingError:
            raise
        except (UnexpectedError, Exception, KeyboardInterrupt):
            self.close()
            raise
        
    def create(self, collection, data, mode='INSERT', **kwargs):
        db, collection = collection.split('.', 1)
        mode = mode.upper()
        if mode in ('INSERT', 'IGNORE'):
            try:
                self.execute(db, collection, 'insert_one', data, **kwargs)
            except ProgrammingError as e:
                if isinstance(e._origin_error, pymongo.errors.DuplicateKeyError):
                    if mode == 'INSERT':
                        raise DuplicateKeyError(str(e._origin_error))
                    else:
                        pass
                else:
                    raise
        elif mode == 'REPLACE':
            m_id = data.get('_id', None)
            if m_id:
                self.execute(
                    db, collection, 'replace_one',
                    {'_id': m_id}, data, upsert=True, **kwargs
                )
            else:
                raise ProgrammingError(
                    'mongodb create mode replace, '
                    'data without _id is not supported')

    def update(self, collection, filters, data, **kwargs):
        db, collection = collection.split('.', 1)
        self.execute(
            db, collection, 'update_many',
            filters, data, **kwargs
        )

    def delete(self, collection, filters, **kwargs):
        db, collection = collection.split('.', 1)
        self.execute(
            db, collection, 'delete_many',
            filters, **kwargs
        )
        
    def filter(self, collection, filters, fields=None,
               order_by=None, limit=1000, **kwargs):
        db, collection = collection.split('.', 1)
        if fields:
            fields_dict = dict([(f, 1) for f in fields or []])
        
            if '_id' not in fields:
                fields_dict['_id'] = 0
        
            return self.execute(
                db, collection, 'find',
                filters, fields_dict, **kwargs
            )
        else:
            return self.execute(
                db, collection, 'find',
                filters, **kwargs
            )
