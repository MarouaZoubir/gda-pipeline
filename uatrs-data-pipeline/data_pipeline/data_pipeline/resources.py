# resources.py

from dagster import resource
import pymysql

@resource
def mysql_conn_resource(_):
    return pymysql.connect(host="localhost",port=3307, user="uatrs_user", password="uatrs_pass", database="uatrs_db")

@resource
def postgres_conn_resource(_):
    import psycopg2
    return psycopg2.connect(host="locahost",port=5433, user="uatrs_user", password="uatrs_pass", dbname="uatrs_db")

@resource
def ge_context_resource(_):
    from great_expectations.data_context import DataContext
    return DataContext("C:/Users/HA/Desktop/stage-gda-real/data-quality/great_expectations/great_expectations.yml")

@resource
def debezium_conn_resource(_):
    return {
        "host": "localhost",
        "port": 8083,
        "kafka_brokers": "kafka:9092",
        "expected_config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.include.list": "uatrs_db"
        }
    }
