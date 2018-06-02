import pytest

import time

from multiprocessing.pool import ThreadPool
from threading import current_thread

from curd import DuplicateKeyError, OperationFailure


def create(session, create_test_table):
    collection = create_test_table(session)

    data = {'id': 100, 'text': 'test'}

    session.create(collection, data)
    with pytest.raises(DuplicateKeyError):
        session.create(collection, data, mode='insert')

    assert data == session.get(collection, [('=', 'id', 100)])

    time.sleep(10)
    data2 = {'id': 100, 'text': 't2'}
    session.create(collection, data2, mode='replace')

    assert data != session.get(collection, [('=', 'id', 100)])
    
    
def update(session, create_test_table):
    collection = create_test_table(session)
    data = {'id': 100, 'text': 'test'}
    session.create(collection, data)
    session.update(collection, {'text': 't2'}, [('=', 'id', data['id'])])
    d = session.get(collection, [('=', 'id', data['id'])])
    assert d['text'] == 't2'
    
    
def delete(session, create_test_table):
    collection = create_test_table(session)
    data = {'id': 100, 'text': 'test'}
    session.create(collection, data)
    time.sleep(10)
    session.delete(collection, [('=', 'id', data['id'])])
    d = session.get(collection, [('=', 'id', data['id'])])
    assert d is None
    
    
def normal_filter(session, create_test_table):
    collection = create_test_table(session)
    for i in range(1, 2000):
        session.create(collection, {'id': i, 'text': 'test'})
        
    items = session.filter(
        collection, [('<=', 'id', 1000)], fields=['text'], limit=None)
    assert len(items) == 1000
    for item in items:
        assert not item.get('id')

    
def filter_with_order_by(session, create_test_table):
    collection = create_test_table(session)
    for i in range(1, 2000):
        session.create(collection, {'id': i, 'text': 'test'})

    items = session.filter(
        collection, [('IN', 'id', (1, 32, 16))],
        fields=['id'], order_by='id')
    assert [item['id'] for item in items] == [1, 16, 32]

    items = session.filter(
        collection, [('IN', 'id', (1, 32, 16))],
        fields=['id'], order_by='-id')
    assert [item['id'] for item in items] == [32, 16, 1]
    
    
def timeout(session, create_test_table):
    collection = create_test_table(session)
    for i in range(1, 2000):
        session.create(collection, {'id': i, 'text': 'test'})

    with pytest.raises(OperationFailure):
        items = session.filter(
            collection, [('IN', 'id', (1, 32, 16))],
            fields=['id'], order_by='id', timeout=0.000001)
        print(items)


def thread_pool(session, create_test_table):
    collection = create_test_table(session)
    
    def create(i):
        session.create(
            collection,
            {'id': i, 'text': current_thread().getName()}
        )
    pool_size = 20
    pool = ThreadPool(pool_size)
    pool.map(create, range(1, 10000))
    pool.terminate()
    
    items = session.filter(collection, [('>=', 'id', 1)], limit=None)
    assert len(items) == 9999
    t_names = set()
    for item in items:
        t_names.add(item['text'])
    assert len(t_names) == pool_size
