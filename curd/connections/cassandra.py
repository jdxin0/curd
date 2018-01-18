import copy
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra import Timeout, OperationTimedOut, InvalidRequest

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
from . import (
    BaseConnection, DEFAULT_FILTER_LIMIT, DEFAULT_TIMEOUT, OP_RETRY_WARNING
)


class CassandraConnection(BaseConnection):
    
    def __init__(self, conf):
        self._conf = conf
        self.cluster, self.session = None, None

        self.max_op_fail_retry = conf.get('max_op_fail_retry', 0)
        self.default_timeout = conf.get('timeout', DEFAULT_TIMEOUT)
        
    def _connect(self, conf):
        conf = copy.deepcopy(conf)

        self.max_op_fail_retry = conf.pop('max_op_fail_retry', 0)
        self.default_timeout = conf.pop('timeout', DEFAULT_TIMEOUT)

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
            result = list(self.session.execute(query, params, **kwargs))
        except (Timeout, OperationTimedOut) as e:
            raise OperationFailure(origin_error=e)
        except InvalidRequest as e:
            raise ProgrammingError(origin_error=e)
        except Exception as e:
            raise UnexpectedError(origin_error=e)
        else:
            return result
        
    def execute(self, query, params=None, retry=None, timeout=None):
        if retry is None:
            retry = self.max_op_fail_retry
            
        if timeout is None:
            timeout = self.default_timeout

        retry_no = 0
        while True:
            try:
                rows = self._execute(query, params, timeout=timeout)
            except OperationFailure as e:
                # self.close()
                if retry_no < retry:
                    logger.warning(OP_RETRY_WARNING.format(str(e)))
                    retry_no += 1
                else:
                    raise
            except ProgrammingError:
                    raise
            except (UnexpectedError, Exception, KeyboardInterrupt):
                self.close()
                raise
            else:
                return [row._asdict() for row in rows]
        
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
