from setuptools import setup, find_packages


setup(
    name='curd',
    version='0.0.8.5',
    url='https://github.com/jdxin0/curd',
    description='db operations',
    author='jdxin0',
    author_email='jdxin00@gmail.com',
    license='MIT',
    keywords='db operations',
    packages=find_packages(),
    install_requires=[
        'cassandra-driver==3.11.0',
        'PyMySQL==0.7.11',
    ],
)
