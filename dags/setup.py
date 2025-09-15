from setuptools import setup, find_packages

setup(
    name='dags',
    version='0.1.0',
    packages=find_packages(where="."),
    install_requires=[
        'apache-airflow',
        'redis'
    ]
)
