# Curd, A Database Client

## Quick start


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

session = Session([mysql_conf])

collection = 'test.test'
item = {'id': 1, 'text': 'test'}

session.create(collection, item)
```


## Install

Install with database driver as extras require (reduce install time)

```
pip install curd[mysql]
pip install curd[cassandra]
```

## Feature
1. Supported databases: cassandra / mysql (tidb `tidb_patch`).
2. Supported operations: `create`, `filter`, `update`, `delete`, `exist`. 
   You can use `execute` for other operations (complex query).
3. Multiple threads/processes supported, you can share `Session` whatever you like.
4. Simple error handling. `ConnectError`, `OperationFailure`, `UnexpectedError`,  `ProgrammingError`, `DuplicateKeyError`.
5. Operation timeout support for mysql.


## Questions that I asked myself
1. Why not orm ?
   
   Single point of truth.
2. Why `execute` ?

   Sql is simpler for complex query.

3. Why fetchall, not paging?
   
   Paging is too heavy due to complex web environments. 
   You should handle it in your application.
    
4. Error handling

   * Retry when operation with `OperationFailure`
     
     errors like mysql
     `MySQL server has gone away`
     `mysql connection timeout`
     `mysql interface error`,
     
     cassandra `OperationFailure`.
            
   * Raise when operation with `UnexpectedError`,  `ProgrammingError`(mostly sql error)
   * Raise when creating connection with `ConnectError`
   * Raise when create item with `DuplicateKeyError`
   
5. Bulk operation
   
   Not decided yet.

