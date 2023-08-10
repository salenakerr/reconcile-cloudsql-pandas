# modules/data_reconciler.py
import uuid
from config_loader import config, get_google_secret
from urllib.parse import quote_plus
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime
import yaml
from google.cloud import secretmanager_v1

def store_preliminary_result():
    # URL-encode the password
    encoded_password = quote_plus(get_google_secret(config['result_postgres']['password']))

    # Create the connection string for the result PostgreSQL database
    result_connection_str = f"postgresql+psycopg2://{config['result_postgres']['user']}:{encoded_password}@{config['result_postgres']['host']}:{config['result_postgres']['port']}/{config['result_postgres']['database']}"

    # Create a SQLAlchemy engine
    engine = create_engine(result_connection_str)

    # Create a preliminary DataFrame for the initial entry
    short_uuid = str(uuid.uuid4())[:8]

    preliminary_df = pd.DataFrame([{
        "reconcile_id": config['reconcile_type'] + '_' + datetime.now().strftime('%Y%m%d_%H%M%S') + '_' + short_uuid,
        "reconcile_type": config['reconcile_type'],
        "release_version": config['release_version'],
        "src_project_id": config['src_project_id'],
        "src_instance_name": config['src_instance_name'],
        "src_database": config['src_database'],
        "src_schema": config['postgres']['schema'],
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

def update_final_result(preliminary_df, result):
    # URL-encode the password
    encoded_password = quote_plus(get_google_secret(config['result_postgres']['password']))

    # Create the connection string for the result PostgreSQL database
    result_connection_str = f"postgresql+psycopg2://{config['result_postgres']['user']}:{encoded_password}@{config['result_postgres']['host']}:{config['result_postgres']['port']}/{config['result_postgres']['database']}"

    # Create a SQLAlchemy engine
    engine = create_engine(result_connection_str)

     # Update the DataFrame with the final reconciliation result
    preliminary_df.loc[:, 'end_time'] = datetime.now()
    preliminary_df.loc[:, 'reconcile_status'] = "success"
    preliminary_df.loc[:, 'updated_at'] = datetime.now()
    preliminary_df.loc[:, 'schema_match'] = result['schema_match']
    preliminary_df.loc[:, 'total_source_record'] = result['total_source_record']
    preliminary_df.loc[:, 'total_target_record'] = result['total_target_record']
    preliminary_df.loc[:, 'match_record'] = len(result['match_record'])
    preliminary_df.loc[:, 'mismatch_record'] = len(result['mismatch_record'])
    preliminary_df.loc[:, 'missing_record'] = len(result['missing_record'])
    preliminary_df.loc[:, 'duplicate_record'] = len(result['duplicate_record'])

    # Convert datetime columns to string for SQL update
    preliminary_df['end_time'] = preliminary_df['end_time'].astype(str)
    preliminary_df['updated_at'] = preliminary_df['updated_at'].astype(str)

    # Create a connection to the PostgreSQL database
    conn = engine.raw_connection()
    cursor = conn.cursor()

    # Update the row using SQL
    for _, row in preliminary_df.iterrows():
        update_query = f"""
        UPDATE tbl_reconcile_summary
        SET 
            end_time = '{row['end_time']}',
            reconcile_status = '{row['reconcile_status']}',
            updated_at = '{row['updated_at']}',
            schema_match = {row['schema_match']},
            total_source_record = {row['total_source_record']},
            total_target_record = {row['total_target_record']},
            match_record = {row['match_record']},
            mismatch_record = {row['mismatch_record']},
            missing_record = {row['missing_record']},
            duplicate_record = {row['duplicate_record']}
        WHERE
            reconcile_id = '{row['reconcile_id']}';
        """
        cursor.execute(update_query)
        
        # Insert new rows into tbl_mismatch_record using SQL
        

        for index, mismatch_record in result['mismatch_record']:
            escaped_mismatch_record = mismatch_record.replace("'", "''")
            short_uuid = str(uuid.uuid4())[:8]
            insert_mismatch_query = f"""
            INSERT INTO tbl_mismatch_record (id, reconcile_id, reason, record_id, fix_status, created_at, updated_at)
            VALUES ('mismatch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{short_uuid}', '{row['reconcile_id']}', '{escaped_mismatch_record}', '{index}', 'open', '{datetime.now()}', '{datetime.now()}');
            """
            cursor.execute(insert_mismatch_query)

        # Insert new rows into tbl_missing_record using SQL
        for index in result['missing_record']:
            short_uuid = str(uuid.uuid4())[:8]
            insert_missing_query = f"""
            INSERT INTO tbl_missing_record (id, reconcile_id, record_id, fix_status, created_at, updated_at)
            VALUES ('missing_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{short_uuid}', '{row['reconcile_id']}', '{index}', 'open', '{datetime.now()}', '{datetime.now()}');
            """
            cursor.execute(insert_missing_query)

        # Insert new rows into tbl_duplicate_record using SQL
        for index in result['duplicate_record']:
            short_uuid = str(uuid.uuid4())[:8]
            insert_dup_query = f"""
            INSERT INTO tbl_dup_record (id, reconcile_id, record_id, fix_status, created_at, updated_at)
            VALUES ('dup_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{short_uuid}', '{row['reconcile_id']}', '{index}', 'open', '{datetime.now()}', '{datetime.now()}');
            """
            cursor.execute(insert_dup_query)
        
        
        conn.commit()

    print(f"Reconciliation result updated in the 'tbl_reconcile_summary' table of the result PostgreSQL database.")

    # Close the cursor and connection
    cursor.close()
    conn.close()

def reconcile_mysql_with_bigquery(mysql_data, bq_data):
    count_src = len(mysql_data)
    count_target = len(bq_data)
    schema_match = check_schema(mysql_data, bq_data)
    match_records, mismatch_records = compare_records(mysql_data, bq_data)
    missing_mysql_records = find_missing_records(mysql_data, bq_data)
    duplicate_mysql_records = find_duplicate_records(mysql_data)
    
    result = {
        'total_source_record': count_src,
        'total_target_record': count_target,
        'schema_match': schema_match,
        'match_record': match_records,
        'mismatch_record': mismatch_records,
        'missing_record': missing_mysql_records,
        'duplicate_record': duplicate_mysql_records
    }

    return result

def reconcile_postgres_with_bigquery(postgres_data, bq_data):
    count_src = len(postgres_data)
    count_target = len(bq_data)
    schema_match = check_schema(postgres_data, bq_data)
    match_records, mismatch_records = compare_records(postgres_data, bq_data)
    missing_postgres_records = find_missing_records(postgres_data, bq_data)
    duplicate_postgres_records = find_duplicate_records(postgres_data)

    
    result = {
        'total_source_record': count_src,
        'total_target_record': count_target,
        'schema_match': schema_match,
        'match_record': match_records,
        'mismatch_record': mismatch_records,
        'missing_record': missing_postgres_records,
        'duplicate_record': duplicate_postgres_records
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
            if row[col] != data2.at[index, col]:
                match = False
                if mismatch_reason != "":
                    mismatch_reason = mismatch_reason+f" ,Column '{col}' mismatch"
                else:
                    mismatch_reason = mismatch_reason+f"Column '{col}' mismatch"
                
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