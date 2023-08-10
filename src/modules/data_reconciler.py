# modules/data_reconciler.py
import uuid
import logging
from config_loader import config, get_google_secret
from urllib.parse import quote_plus
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime
import yaml
from google.cloud import secretmanager_v1

def reconcile_mysql_with_bigquery(source_data, bq_data):
    logging.info("BEGIN: Performing data reconciliation...")

    logging.info("BEGIN: Count check...")
    count_src = len(source_data)
    count_target = len(bq_data)
    logging.info("END: Count check completed successfully")

    logging.info("BEGIN: Schema check...")
    schema_match = check_schema(source_data, bq_data)
    schema_match_result = "matched" if schema_match else "mismatched"
    logging.info("END: Schema check completed successfully")
    
    logging.info("BEGIN: Match/mismatch check...")
    match_records, mismatch_records = compare_records(source_data, bq_data)
    logging.info("END: Match/mismatch check completed successfully")

    logging.info("BEGIN: Missing check...")
    missing_records = find_missing_records(source_data, bq_data)
    logging.info("END: Missing check completed successfully")
    
    logging.info("BEGIN: Duplicate check...")
    duplicate_records = find_duplicate_records(source_data)
    logging.info("END: Duplicate check completed successfully")

    logging.info("END: Data reconciliation completed successfully")
    
    result = {
        'total_source_record': count_src,
        'total_target_record': count_target,
        'schema_match': schema_match_result,
        'match_record': match_records,
        'mismatch_record': mismatch_records,
        'missing_record': missing_records,
        'duplicate_record': duplicate_records
    }

    return result

def reconcile_postgres_with_bigquery(source_data, bq_data):
    logging.info("BEGIN: Performing data reconciliation...")

    logging.info("BEGIN: Count check...")
    count_src = len(source_data)
    count_target = len(bq_data)
    logging.info("END: Count check completed successfully")

    logging.info("BEGIN: Schema check...")
    schema_match = check_schema(source_data, bq_data)
    schema_match_result = "matched" if schema_match else "mismatched"
    logging.info("END: Schema check completed successfully")
    
    logging.info("BEGIN: Match/mismatch check...")
    match_records, mismatch_records = compare_records(source_data, bq_data)
    logging.info("END: Match/mismatch check completed successfully")

    logging.info("BEGIN: Missing check...")
    missing_records = find_missing_records(source_data, bq_data)
    logging.info("END: Missing check completed successfully")
    
    logging.info("BEGIN: Duplicate check...")
    duplicate_records = find_duplicate_records(source_data)
    logging.info("END: Duplicate check completed successfully")

    logging.info("END: Data reconciliation completed successfully")
    
    result = {
        'total_source_record': count_src,
        'total_target_record': count_target,
        'schema_match': schema_match_result,
        'match_record': match_records,
        'mismatch_record': mismatch_records,
        'missing_record': missing_records,
        'duplicate_record': duplicate_records
    }

    return result

def check_schema(data1, data2):
    # Compare column names and data types
    return data1.columns.tolist() == data2.columns.tolist()

def compare_records(data1, data2):
    common_columns = set(data1.columns) & set(data2.columns)
    match_records = []
    mismatch_records = []
    
    for index, row in data1.iterrows():
        match = True
        mismatch_reason = ""
        
        for col in common_columns:
            value1 = row[col]
            value2 = data2.at[index, col]

            # Convert timestamp/datetime to a common format
            if isinstance(value1, (pd.Timestamp, datetime)):
                value1 = value1.strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(value2, (pd.Timestamp, datetime)):
                value2 = value2.strftime('%Y-%m-%d %H:%M:%S')

            if value1 != value2:
                match = False
                if mismatch_reason != "":
                    mismatch_reason = mismatch_reason + f", Column '{col}' mismatch"
                else:
                    mismatch_reason = mismatch_reason + f"Column '{col}' mismatch"
                
        if match:
            match_records.append(index)
        else:
            mismatch_records.append((index, mismatch_reason))
    
    return match_records, mismatch_records


def find_missing_records(data1, data2):
    missing_records = data1.index.difference(data2.index)
    return missing_records.tolist()

def find_duplicate_records(data):
    # Find duplicates and return boolean array
    duplicate_mask = data.duplicated()

    # Use boolean indexing to extract duplicate rows
    duplicate_records = data[duplicate_mask]

    return duplicate_records