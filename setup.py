from setuptools import setup


setup(
    name='curd',
    version='0.0.1',
    url='https://github.com/jdxin0/curd',
    description='db operations',
    author='jdxin0',
    author_email='jdxin00@gmail.com',
    license='MIT',
    keywords='db operations',
    packages=[
        'curd',
    ],
    install_requires=[
        'cassandra-driver',
        'pymysql',
    ],
)
