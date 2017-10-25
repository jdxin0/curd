import pymysql

from ..errors import (
    UnexpectedError, OperationFailure, ProgrammingError,
    ConnectError,
    DuplicateKeyError
)
from . import logger
from .utils.sql import (
    query_parameters_from_create,
    query_parameters_from_update,
    query_parameters_from_delete,
    query_parameters_from_filter
)
from . import BaseConnection


# https://www.briandunning.com/error-codes/?source=MySQL

PE_MYSQL_ERROR_CODE_LIST = [
    1265,  # Data truncated, tidb error
] + list(range(1046, 1076))


PE_DUPLICATE_ENTRY_KEY_ERROR_CODE = 1062

OF_MYSQL_ERROR_CODE_LIST = [0, 1040, 2006, 2013]
OF_MYSQL_RETRY_ERROR_CODE_LIST = [
    0,  # mysql interface error
    1040,  # mysql too many connections
    2006,  # mysql gone away
    2013,  # mysql connection timeout
]
MAX_OF_RETRY = 3

TIDB_TRY_AGAIN_LATER_ERROR_CODE = 1105
OF_TIDB_ERROR_CODE_LIST = OF_MYSQL_ERROR_CODE_LIST + \
    [TIDB_TRY_AGAIN_LATER_ERROR_CODE]
OF_TIDB_RETRY_ERROR_CODE_LIST = OF_MYSQL_RETRY_ERROR_CODE_LIST + \
    [TIDB_TRY_AGAIN_LATER_ERROR_CODE]


class MysqlConnection(BaseConnection):
    pe_mysql_error_code_list = PE_MYSQL_ERROR_CODE_LIST
    pe_duplicate_entry_key_error_code = PE_DUPLICATE_ENTRY_KEY_ERROR_CODE
    
    of_mysql_error_code_list = OF_MYSQL_ERROR_CODE_LIST
    of_mysql_retry_error_code_list = OF_MYSQL_RETRY_ERROR_CODE_LIST
    max_of_retry = MAX_OF_RETRY
    
    def __init__(self, conf):
        self._conf = conf
        self.conn, self.cursor = None, None
    
    def _connect(self, conf):
        conf['use_unicode'] = True
        conf['charset'] = 'utf8mb4'
        conf['autocommit'] = True

        conn = pymysql.connect(**conf)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        return conn, cursor
    
    def connect(self, conf):
        try:
            return self._connect(conf)
        except Exception as e:
            raise ConnectError(origin_error=e)
    
    def close(self):
        if self.cursor:
            try:
                self.cursor.close()
            except Exception as e:
                logger.warning(str(e))
        if self.conn:
        
            try:
                self.conn.close()
            except Exception as e:
                logger.warning(str(e))

        self.conn, self.cursor = None, None
        
    def _execute(self, query, params):
        if self.cursor and self.conn:
            pass
        else:
            self.conn, self.cursor = self.connect(self._conf)
        
        try:
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except pymysql.err.ProgrammingError as e:
            raise ProgrammingError(origin_error=e)
        except Exception as e:
            if isinstance(e.args, tuple) and len(e.args) >= 1:
                if e.args[0] in self.pe_mysql_error_code_list:
                    raise ProgrammingError(origin_error=e)
                elif e.args[0] in self.of_mysql_error_code_list:
                    raise OperationFailure(origin_error=e)
                else:
                    raise UnexpectedError(origin_error=e)
            else:
                raise UnexpectedError(origin_error=e)
            
    def execute(self, query, params=None, retry=0):
        retry_no = 0
        of_retry_count = 0
        if retry_no <= retry:
            try:
                rows = list(self._execute(query, params))
                return rows
            except OperationFailure as e:
                # deal with retry
                if e._origin_error.args[0] in self.of_mysql_retry_error_code_list:
                    if of_retry_count < MAX_OF_RETRY:
                        logger.warning(str(e._origin_error))
                        of_retry_count += 1
                        
                        self.close()
                        retry_no -= 1
                    else:
                        raise
    
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
        try:
            self.execute(query, params, **kwargs)
        except ProgrammingError as e:
            if e._origin_error.args[0] == self.pe_duplicate_entry_key_error_code:
                raise DuplicateKeyError(str(e._origin_error))
            else:
                raise

    def update(self, collection, data, filters, **kwargs):
        filters = self._check_filters(filters)
        query, params = query_parameters_from_update(collection, filters, data)
        self.execute(query, params, **kwargs)

    def delete(self, collection, filters, **kwargs):
        filters = self._check_filters(filters)
        query, params = query_parameters_from_delete(collection, filters)
        self.execute(query, params, **kwargs)
        
    def filter(self, collection, filters=None, fields=None,
               order_by=None, limit=1000, **kwargs):
        filters = self._check_filters(filters)
        query, params = query_parameters_from_filter(
            collection, filters, fields, order_by, limit)
        rows = self.execute(query, params, **kwargs)
        return rows
    
    def patch_execute_as_tidb(self):
        self.of_mysql_error_code_list = OF_TIDB_ERROR_CODE_LIST
        self.of_mysql_retry_error_code_list = OF_TIDB_RETRY_ERROR_CODE_LIST
