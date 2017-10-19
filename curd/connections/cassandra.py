from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra import Timeout, InvalidRequest

from ..errors import (
    UnexpectedError, OperationFailure, ProgrammingError,
    ConnectError,
    DuplicateKeyError
)
from . import logger
from .utils.cql import (
    query_parameters_from_create,
    query_parameters_from_update,
    query_parameters_from_delete,
    query_parameters_from_filter,
)
from . import BaseConnection, DEFAULT_FILTER_LIMIT

CASSANDRA_EXECUTE_TIMEOUT = 3600 * 10


class CassandraConnection(BaseConnection):
    
    def __init__(self, conf):
        self._conf = conf
        self.cluster, self.session = None, None
        
    def _connect(self, conf):
        if conf.get('username', None):
            auth_provider = PlainTextAuthProvider(
                username=conf['username'], password=conf['password']
            )
            cluster = Cluster(conf['hosts'], auth_provider=auth_provider)
        else:
            cluster = Cluster(conf['hosts'])
        session = cluster.connect()
        return cluster, session
    
    def connect(self, conf):
        try:
            return self._connect(conf)
        except Exception as e:
            raise ConnectError(origin_error=e)

    def close(self):
        if self.session:
            try:
                self.session.shutdown()
            except Exception as e:
                logger.warning(str(e))
        if self.cluster:
            try:
                self.cluster.shutdown()
            except Exception as e:
                logger.warning(str(e))

        self.cluster, self.session = None, None
        
    def _execute(self, query, params, **kwargs):
        if self.cluster and self.session:
            pass
        else:
            self.cluster, self.session = self.connect(self._conf)
        
        try:
            return self.session.execute(query, params, **kwargs)
        except Timeout as e:
            raise OperationFailure(origin_error=e)
        except InvalidRequest as e:
            raise ProgrammingError(origin_error=e)
        except Exception as e:
            raise UnexpectedError(origin_error=e)
        
    def execute(self, query, params=None, retry=0, timeout=CASSANDRA_EXECUTE_TIMEOUT):
        retry_no = 0
        if retry_no <= retry:
            try:
                rows = self._execute(query, params, timeout=timeout)
                return [row._asdict() for row in rows]
            except OperationFailure as e:
                if retry_no < retry:
                    logger.warning('retry@' + str(e))
                else:
                    raise
            except ProgrammingError:
                    raise
            except (UnexpectedError, Exception, KeyboardInterrupt):
                self.close()
                raise
            
            retry_no += 1
        
    def create(self, collection, data, mode='INSERT', **kwargs):
        query, params = query_parameters_from_create(
            collection, data, mode.upper())
        rows = self.execute(query, params, **kwargs)
        if rows and mode.upper() != 'IGNORE' and not rows[0].get('applied', True):
            raise DuplicateKeyError

    def update(self, collection, data, filters, **kwargs):
        filters = self._check_filters(filters)
        query, params = query_parameters_from_update(collection, filters, data)
        self.execute(query, params, **kwargs)
        
    def delete(self, collection, filters, **kwargs):
        filters = self._check_filters(filters)
        query, params = query_parameters_from_delete(collection, filters)
        self.execute(query, params, **kwargs)
        
    def filter(self, collection, filters=None, fields=None,
               order_by=None, limit=DEFAULT_FILTER_LIMIT, **kwargs):
        filters = self._check_filters(filters)
        query, params = query_parameters_from_filter(
            collection, filters, fields, order_by, limit)
        rows = self.execute(query, params, **kwargs)
        return rows
