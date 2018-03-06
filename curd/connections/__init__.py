import logging

from ..errors import ProgrammingError


logger = logging.Logger('curd')

DEFAULT_FILTER_LIMIT = None
DEFAULT_TIMEOUT = 3600 * 10

CREATE_MODE = ('INSERT', 'IGNORE', 'REPLACE')
FILTER_OP = ('<', '>', '>=', '<=', '=', '!=', 'IN')
CURD_FUNCTIONS = (
    'create', 'update', 'get', 'delete', 'filter', 'exist', 'execute'
)

OP_RETRY_WARNING = 'RETRY: {}'


class BaseConnection(object):
    def _check_filters(self, filters):
        if filters is None:
            filters = []
        new_filters = []
        for op, k, v in filters:
            if op.upper() not in FILTER_OP:
                raise ProgrammingError('Not support filter Operator')
            else:
                new_filters.append((op.upper(), k, v))
        return new_filters
    
    def create(self, collection, data, mode='INSERT', **kwargs):
        raise NotImplementedError

    def update(self, collection, data, filters, **kwargs):
        raise NotImplementedError

    def delete(self, collection, filters, **kwargs):
        raise NotImplementedError
    
    def filter(self, collection, filters=None, fields=None,
               order_by=None, limit=DEFAULT_FILTER_LIMIT, **kwargs):
        raise NotImplementedError
    
    def get(self, collection, filters=None, fields=None, **kwargs):
        rows = self.filter(collection, filters, fields, limit=1, **kwargs)
        if rows:
            return rows[0]
        else:
            return None

    def exist(self, collection, filters, **kwargs):
        if filters:
            fields = [filters[0][1]]
            data = self.get(collection, filters, fields=fields, **kwargs)
            if data:
                return True
            else:
                return False
        else:
            raise ProgrammingError('exist without filter is not supported')
