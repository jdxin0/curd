from cassandra.cqlengine.statements import (
    InsertStatement,
    UpdateStatement,
    SelectStatement as _SelectStatement,
    DeleteStatement,
    WhereClause, AssignmentClause,
    six
)
from cassandra.cqlengine.operators import BaseWhereOperator


class SelectStatement(_SelectStatement):
    def __unicode__(self):
        qs = ['SELECT']
        if self.distinct_fields:
            if self.count:
                qs += ['DISTINCT COUNT({0})'.format(', '.join(['"{0}"'.format(f) for f in self.distinct_fields]))]
            else:
                qs += ['DISTINCT {0}'.format(', '.join(['"{0}"'.format(f) for f in self.distinct_fields]))]
        elif self.count:
            qs += ['COUNT(*)']
        else:
            qs += [', '.join(['"{0}"'.format(f) for f in self.fields]) if self.fields else '*']
        qs += ['FROM', self.table]

        if self.where_clauses:
            qs += [self._where]

        if self.order_by and not self.count:
            segs = []
            for o in self.order_by:
                if o.startswith('-'):
                    segs.append(six.text_type(o[1:]) + ' DESC')
                else:
                    segs.append(six.text_type(o))
            
            qs += ['ORDER BY {0}'.format(', '.join(segs))]

        if self.limit:
            qs += ['LIMIT {0}'.format(self.limit)]

        if self.allow_filtering:
            qs += ['ALLOW FILTERING']

        return ' '.join(qs)


CQL_SYMBOL = dict(
    [(op.cql_symbol, op.symbol) for op in BaseWhereOperator.opmap.values()]
)


def where_clauses_from_filters(filters):
    where_clauses = []
    for op, k, v in filters:
        where_clause = WhereClause(
            k, BaseWhereOperator.get_operator(CQL_SYMBOL[op.upper()])(), v)
        where_clauses.append(where_clause)
    return where_clauses


def assignment_clauses_from_data(data):
    assignment_clauses = []
    for k, v in data.items():
        assignment_clauses.append(AssignmentClause(k, v))
    return assignment_clauses


def query_parameters_from_create(collection, data, mode='INSERT'):
    assignment_clauses = assignment_clauses_from_data(data)
    if mode == 'REPLACE':
        statement = InsertStatement(
            table=collection, assignments=assignment_clauses,
        )
    else:
        statement = InsertStatement(
            table=collection, assignments=assignment_clauses,
            if_not_exists=True
        )
    return str(statement), statement.get_context()


def query_parameters_from_update(collection, filters, data):
    where_clauses = where_clauses_from_filters(filters)
    assignment_clauses = assignment_clauses_from_data(data)
    statement = UpdateStatement(
        table=collection, assignments=assignment_clauses,
        where=where_clauses, if_exists=True
    )
    return str(statement), statement.get_context()


def query_parameters_from_delete(collection, filters):
    where_clauses = where_clauses_from_filters(filters)
    statement = DeleteStatement(
        table=collection, where=where_clauses
    )
    return str(statement), statement.get_context()


def query_parameters_from_filter(
    collection, filters, fields=None, order_by=None, limit=None
):
    where_clauses = where_clauses_from_filters(filters)
    statement = SelectStatement(
        table=collection, fields=fields,
        where=where_clauses, allow_filtering=True,
        order_by=order_by, limit=limit
    )
    return str(statement), statement.get_context()
