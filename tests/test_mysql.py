from .operations import (
    create, delete, normal_filter, filter_with_order_by, thread_pool, update
)

from curd import Session
from .conf import mysql_conf

    
def create_test_table(session):
    session.execute('DROP DATABASE IF EXISTS `curd`')
    session.execute('CREATE DATABASE `curd` DEFAULT CHARACTER SET utf8mb4')
    session.execute('CREATE TABLE `curd`.`test` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT, `text` text, PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=300000 DEFAULT CHARSET=utf8mb4')
    return 'curd.test'


def test_mysql():
    session = Session([mysql_conf])
    create(session, create_test_table)
    update(session, create_test_table)
    delete(session, create_test_table)
    normal_filter(session, create_test_table)
    filter_with_order_by(session, create_test_table)
    thread_pool(session, create_test_table)
