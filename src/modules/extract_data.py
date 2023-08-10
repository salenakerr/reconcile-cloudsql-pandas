# modules/extract_data.py
from config_loader import config, get_google_secret
import pandas as pd
import mysql.connector
import psycopg2
from google.cloud import bigquery
from google.cloud import secretmanager_v1


def bq_query(query, project_id):
    # Create a BigQuery client
    client = bigquery.Client(project=project_id)

    # Execute the query and return the result as a DataFrame
    query_job = client.query(query)
    result_df = query_job.to_dataframe()

    return result_df

def mysql_query(query, db_config):
    # Create a MySQL connection
    mysql_conn = mysql.connector.connect(
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=get_google_secret(db_config['password']),  # Use the retrieved password
        database=config['src_database']
    )

    # Execute the query and return the result as a DataFrame
    result_df = pd.read_sql_query(query, mysql_conn)

    # Close the MySQL connection
    mysql_conn.close()

    return result_df

def postgres_query(query, db_config):
    # Create a PostgreSQL connection
    postgres_conn = psycopg2.connect(
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=get_google_secret(db_config['password']),  # Use the retrieved password
        database=config['src_database']
    )

    # Execute the query and return the result as a DataFrame
    result_df = pd.read_sql_query(query, postgres_conn)

    # Close the PostgreSQL connection
    postgres_conn.close()

    return result_df
