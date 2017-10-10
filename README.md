## curd client

supported databases:
* cassandra
* mysql


### opretaions
```python
from curd import Session

mysql_conf = {
    'type': 'mysql',
    'conf': {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'user',
        'password': 'password',
    }
}

cassandra_conf = {
    'type': 'cassandra',
    'conf': {
        'hosts': ['127.0.0.1'],
    }
}


session = Session([mysql_conf])

session.create('test.test', {'id': 1, 'text': 'test'})
```