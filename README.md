## curd client


### Install
```
pip install curd[mysql]
pip install curd[cassandra]
```

### Supported databases:
* cassandra
* mysql / tidb (some enhancements)


### Concurrency
thread supported

[**Notice**] you need a new session object when using multiple process.


### Operations
* Support operations: create, filter, update, delete, exist
* Other operations: execute

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