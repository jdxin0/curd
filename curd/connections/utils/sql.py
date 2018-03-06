from datetime import datetime, timezone


class BaseClause(object):
    def __init__(self, field, value):
        self._field = field
        self._value = value
        
    @property
    def field(self):
        return '.'.join(
            ['`{}`'.format(i) for i in self._field.replace('`', '').split('.')])
    
    @property
    def value(self):
        if isinstance(self._value, datetime) and self._value.tzinfo:
            return self._value.astimezone(tz=timezone.utc).replace(tzinfo=None)
        elif isinstance(self._value, list) or isinstance(self._value, tuple):
            value = []
            for v in self._value:
                if isinstance(self._value, datetime) and self._value.tzinfo:
                    value.append(
                        v.astimezone(tz=timezone.utc).replace(tzinfo=None)
                    )
                else:
                    value.append(v)
            return value
        else:
            return self._value
        
        
class WhereClause(BaseClause):
    def __init__(self, field, operator, value):
        super().__init__(field, value)
        self._operator = operator
        if value is None:
            if self._operator == '=':
                self._operator = 'IS'
            elif self._operator == '!=':
                self._operator = 'IS NOT'
        
    @property
    def operator(self):
        return self._operator
        
        
class FieldClause(BaseClause):
    def __init__(self, field):
        self._field = field

    
class AssignmentClause(BaseClause):
    pass


class BaseSQLStatement(object):
    def __init__(self):
        self.query = None
        self.params = []
    
    def generate_query_field(self, table):
        return table.field
    
    def generate_query_where(self, where):
        query = ''
        
        if where:
            query += 'WHERE '
            segs = []
            for where_clause in where:
                if where_clause.operator == 'IN':
                    value_count = len(where_clause.value)
                    segs.append(
                        '{} {} {}'.format(
                            where_clause.field,
                            where_clause.operator,
                            '({})'.format(', '.join(['%s']*value_count))
                        )
                    )
                    
                    for v in where_clause.value:
                        self.params.append(v)
                else:
                    segs.append(
                        '{} {} {}'.format(
                            where_clause.field, where_clause.operator, '%s'
                        )
                    )
                    self.params.append(where_clause.value)
            query += ' AND '.join(segs)
        return query

    def generate_query_fields(self, fields):
        if fields:
            return ', '.join([field_clause.field for field_clause in fields])
        else:
            return '*'
        
    def generate_query_limit(self, limit):
        if limit:
            return 'LIMIT {}'.format(limit)
        else:
            return ''
        
    def generate_query_order_by(self, order_by):
        query = ''
        if order_by:
            query += 'ORDER BY '
            segs = []
            for field_clause in order_by:
                if field_clause._field.startswith('-'):
                    field_clause._field = field_clause._field[1:]
                    seg = field_clause.field + ' DESC'
                else:
                    seg = field_clause.field
                segs.append(seg)
            query += ', '.join(segs)
        return query


class SelectStatement(BaseSQLStatement):
    BASE_QUERY = 'SELECT {} FROM {} {}'
    
    def __init__(self, table, fields=None, where=None, order_by=None, limit=None):
        super().__init__()

        self.table = table
        self.fields = fields
        self.where = where
        self.order_by = order_by
        self.limit = limit
        
    def as_sql(self):
        query_table = self.generate_query_field(self.table)
        query_fields = self.generate_query_fields(self.fields)
        query_where = self.generate_query_where(self.where)
        query_order_by = self.generate_query_order_by(self.order_by)
        query_limit = self.generate_query_limit(self.limit)
    
        extra_query = ' '.join([
            i for i in [query_where, query_order_by, query_limit] if i
        ])

        self.query = self.BASE_QUERY.format(
            query_fields, query_table, extra_query
        )

        return self.query, self.params
    
    
class DeleteStatement(BaseSQLStatement):
    BASE_QUERY = 'DELETE FROM {} {}'

    def __init__(self, table, where=None):
        super().__init__()
        self.table = table
        self.where = where
    
    def as_sql(self):
        query_table = self.generate_query_field(self.table)
        query_where = self.generate_query_where(self.where)
        extra_query = query_where
    
        self.query = self.BASE_QUERY.format(
            query_table, extra_query
        )
        return self.query, self.params
    
    
class CreateStatement(BaseSQLStatement):
    BASE_QUERY = '{} INTO {} ({}) VALUES ({})'
    
    def __init__(self, table, assignments, mode):
        super().__init__()
        self.table = table
        self.assignments = assignments
        self.mode = mode
        
    def generate_query_mode(self, mode):
        if mode == 'INSERT':
            return 'INSERT'
        elif mode == 'IGNORE':
            return 'INSERT IGNORE'
        elif mode == 'REPLACE':
            return 'REPLACE'
        
    def generate_query_fields_values(self, assignments):
        query_fields = ', '.join([a.field for a in assignments])
        query_values = ', '.join(['%s']*len(assignments))
        for a in assignments:
            self.params.append(a.value)
        return query_fields, query_values
    
    def as_sql(self):
        
        query_mode = self.generate_query_mode(self.mode)
        
        query_table = self.generate_query_field(self.table)
        
        query_fields, query_values = self.generate_query_fields_values(
            self.assignments)
        
        self.query = self.BASE_QUERY.format(
            query_mode, query_table, query_fields, query_values
        )
        return self.query, self.params
    
    
class UpdateStatement(BaseSQLStatement):
    BASE_QUERY = 'UPDATE {} SET {} {}'
    
    def __init__(self, table, assignments, where=None):
        super().__init__()
        self.table = table
        self.assignments = assignments
        self.where = where
        
    def generate_query_fields_values(self, assignments):
        query = ', '.join(
            [a.field + '=%s' for a in assignments]
        )
        for a in assignments:
            self.params.append(a.value)
        return query

    def as_sql(self):
        query_table = self.generate_query_field(self.table)
    
        query_fields_values = self.generate_query_fields_values(
            self.assignments)

        query_where = self.generate_query_where(self.where)
    
        self.query = self.BASE_QUERY.format(
            query_table, query_fields_values, query_where
        )
        return self.query, self.params
    

def where_clauses_from_filters(filters):
    where_clauses = []
    for op, k, v in filters:
        where_clause = WhereClause(k, op, v)
        where_clauses.append(where_clause)
    return where_clauses


def assignment_clauses_clauses_from_filters(data):
    assignment_clauses = []
    for k, v in data.items():
        clause = AssignmentClause(k, v)
        assignment_clauses.append(clause)
    return assignment_clauses


def query_parameters_from_create(collection, data, mode='INSERT'):
    table = FieldClause(collection)
    assignments = assignment_clauses_clauses_from_filters(data)
    query, params = CreateStatement(table, assignments, mode).as_sql()
    return query, params


def query_parameters_from_update(collection, filters, data):
    table = FieldClause(collection)
    assignments = assignment_clauses_clauses_from_filters(data)
    where = where_clauses_from_filters(filters)
    query, params = UpdateStatement(table, assignments, where).as_sql()
    return query, params


def query_parameters_from_get(collection, filters, fields=None):
    table = FieldClause(collection)
    where = where_clauses_from_filters(filters)
    fields = [FieldClause(f) for f in (fields or [])]
    query, params = SelectStatement(table, fields, where, limit=1).as_sql()
    return query, params


def query_parameters_from_delete(collection, filters):
    table = FieldClause(collection)
    where = where_clauses_from_filters(filters)
    query, params = DeleteStatement(table, where).as_sql()
    return query, params


def query_parameters_from_filter(
        collection, filters, fields=None, order_by=None, limit=None):
    table = FieldClause(collection)
    where = where_clauses_from_filters(filters)
    fields = [FieldClause(f) for f in (fields or [])]
    
    if order_by is None:
        order_by = []
    elif isinstance(order_by, str):
        order_by = [order_by]
    order_by = [FieldClause(f) for f in order_by]
    
    query, params = SelectStatement(
        table, fields, where, order_by, limit).as_sql()
    return query, params
