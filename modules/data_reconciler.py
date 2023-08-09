# data_reconciler.py
from config_loader import config
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime
import yaml
from google.cloud import secretmanager_v1

def store_preliminary_result(result_postgres_config):
    # Create the connection string for the result PostgreSQL database
    result_connection_str = f"postgresql+psycopg2://{result_postgres_config['user']}:{get_google_secret(result_postgres_config['password_secret_ref'])}@{result_postgres_config['host']}:{result_postgres_config['port']}/{result_postgres_config['database']}"

    # Create a SQLAlchemy engine
    engine = create_engine(result_connection_str)

    # Create a preliminary DataFrame for the initial entry
    preliminary_df = pd.DataFrame([{
        "reconcile_id": config['reconcile_type'] + '_' + datetime.now().strftime('%Y%m%d_%H%M%S'),
        "reconcile_type": config['reconcile_type'],
        "release_version": config['release_version'],
        "src_project_id": config['src_project_id'],
        "src_instance_name": config['src_instance_name'],
        "src_database": config['src_database'],
        "src_schema": config['src_schema'],
        "src_table": config['src_table'],
        "target_dataset": config['target_dataset'],
        "target_table": config['target_table'],
        "condition_key": config['condition_key'],
        "condition_start": config['condition_start'],
        "condition_end": config['condition_end'],
        "start_time": datetime.now(),
        "end_time": None,
        "reconcile_status": "in-progress",
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    }])

    # Store the preliminary DataFrame in the tbl_reconcile_summary table
    preliminary_df.to_sql('tbl_reconcile_summary', engine, index=False, if_exists='append')

    return preliminary_df

def update_final_result(preliminary_df, result, result_postgres_config):
    # Create the connection string for the result PostgreSQL database
    result_connection_str = f"postgresql+psycopg2://{result_postgres_config['user']}:{get_google_secret(result_postgres_config['password_secret_ref'])}@{result_postgres_config['host']}:{result_postgres_config['port']}/{result_postgres_config['database']}"

    # Create a SQLAlchemy engine
    engine = create_engine(result_connection_str)

    # Replace the "None" values in the result dictionary with appropriate values
    result['end_time'] = datetime.now()
    result['reconcile_status'] = "success"
    result['updated_at'] = datetime.now()

    # Create DataFrame from the updated reconciliation result
    summary_df = pd.DataFrame([result])

    # Update the entry with the final reconciliation result
    summary_df.to_sql('tbl_reconcile_summary', engine, index=False, if_exists='append', method='multi', chunksize=1000)

    print(f"Reconciliation result stored in the 'tbl_reconcile_summary' table of the result PostgreSQL database.")

def reconcile_mysql_with_bigquery(mysql_data, bq_data):

    IMPLEMENT_RECONCILE


    return {
        'count_src': count_src,
        'count_target': count_target,
        'schema_match': schema_match,
        'match_records': match_records,
        'mismatch_records': mismatch_records,
        'missing_mysql_records': missing_mysql_records,
        'duplicate_mysql_records': duplicate_mysql_records
        }


def reconcile_postgres_with_bigquery(postgres_data, bq_data):

    IMPLEMENT_RECONCILE


    return {
        'count_src': count_src,
        'count_target': count_target,
        'schema_match': schema_match,
        'match_records': match_records,
        'mismatch_records': mismatch_records,
        'missing_postgres_records': missing_postgres_records,
        'duplicate_postgres_records': duplicate_postgres_records
        }
    
