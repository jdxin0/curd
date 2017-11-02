from .operations import (
    create, delete, normal_filter, thread_pool, update, timeout
)

from curd import Session
from .conf import cassandra_conf

    
def create_test_table(session):
    session.execute('DROP KEYSPACE IF EXISTS curd')
    session.execute("CREATE KEYSPACE curd WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute('CREATE TABLE curd.test (id int PRIMARY KEY, text text)')
    return 'curd.test'


def test_cassandra():
    session = Session([cassandra_conf])
    create(session, create_test_table)
    update(session, create_test_table)
    delete(session, create_test_table)
    normal_filter(session, create_test_table)
    thread_pool(session, create_test_table)
    timeout(session, create_test_table)
