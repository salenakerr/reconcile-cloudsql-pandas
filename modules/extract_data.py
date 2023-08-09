# extract_data.py
from config_loader import config
import pandas as pd
import mysql.connector
import psycopg2
from google.cloud import bigquery
from google.cloud import secretmanager_v1
import yaml

def get_google_secret(secret_ref):
    # Authenticate the Google Cloud Service Account
    client = secretmanager_v1.SecretManagerServiceClient()

    # Retrieve the password from Secret Manager
    secret_response = client.access_secret_version(name=secret_ref)
    password = secret_response.payload.data.decode("UTF-8")

    return password

def bq_query(query, project_id):
    # Create a BigQuery client
    client = bigquery.Client(project=project_id)

    # Execute the query and return the result as a DataFrame
    query_job = client.query(query)
    result_df = query_job.to_dataframe()

    return result_df

def mysql_query(query):
    # Create a MySQL connection
    mysql_conn = mysql.connector.connect(
        host=config['mysql']['host'],
        port=config['mysql']['port'],
        user=config['mysql']['user'],
        password=get_google_secret(config['mysql']['password_secret_ref']),  # Use the retrieved password
        database=config['mysql']['database']
    )

    # Execute the query and return the result as a DataFrame
    result_df = pd.read_sql_query(query, mysql_conn)

    # Close the MySQL connection
    mysql_conn.close()

    return result_df

def postgres_query(query):
    # Create a PostgreSQL connection
    postgres_conn = psycopg2.connect(
        host=config['postgres']['host'],
        port=config['postgres']['port'],
        user=config['postgres']['user'],
        password=get_google_secret(config['postgres']['password_secret_ref']),  # Use the retrieved password
        database=config['postgres']['database']
    )

    # Execute the query and return the result as a DataFrame
    result_df = pd.read_sql_query(query, postgres_conn)

    # Close the PostgreSQL connection
    postgres_conn.close()

    return result_df
