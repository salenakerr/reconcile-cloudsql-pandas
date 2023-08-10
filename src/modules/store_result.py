# modules/store_result.py
import logging
import uuid
from config_loader import config, get_google_secret
from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from google.cloud import secretmanager_v1

def store_preliminary_result():
    try:
        logging.info("BEGIN: Storing prelimiary data reconciliation result...")
        encoded_password = quote_plus(get_google_secret(config['result_postgres']['password']))

        result_connection_str = f"postgresql+psycopg2://{config['result_postgres']['user']}:{encoded_password}@{config['result_postgres']['host']}:{config['result_postgres']['port']}/{config['result_postgres']['database']}"
        engine = create_engine(result_connection_str)

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
        "primary_key": config['primary_key'],
        "condition_key": config['condition_key'],
        "condition_start": config['condition_start'],
        "condition_end": config['condition_end'],
        "start_time": datetime.now(),
        "end_time": None,
        "reconcile_status": "in-progress",
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    }])

        preliminary_df.to_sql('tbl_reconcile_summary', engine, index=False, if_exists='append')
        logging.info("END: Store prelimiary data reconciliation result completed successfully for reconcileId: " + preliminary_df['reconcile_id'].values[0])
        return preliminary_df
    except Exception as e:
        logging.error("END: An error occurred during store prelimiary data reconciliation result:", exc_info=True)
        print(f"Error in store_preliminary_result: {e}")
        return None

def update_final_result(preliminary_df, result):
    try:
        logging.info("BEGIN: Updating data reconciliation result for reconcileId: " + preliminary_df['reconcile_id'].values[0])

        encoded_password = quote_plus(get_google_secret(config['result_postgres']['password']))

        result_connection_str = f"postgresql+psycopg2://{config['result_postgres']['user']}:{encoded_password}@{config['result_postgres']['host']}:{config['result_postgres']['port']}/{config['result_postgres']['database']}"
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
            logging.info("BEGIN: Updating tbl_reconcile_summary for reconcileId: " + preliminary_df['reconcile_id'].values[0])
            update_query = f"""
            UPDATE tbl_reconcile_summary
            SET 
                end_time = '{row['end_time']}',
                reconcile_status = '{row['reconcile_status']}',
                updated_at = '{row['updated_at']}',
                schema_match = '{row['schema_match']}',
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
            logging.info("END: Updating tbl_reconcile_summary for reconcileId: " + preliminary_df['reconcile_id'].values[0])
            
            logging.info("BEGIN: Inserting tbl_mismatch_record for reconcileId: " + preliminary_df['reconcile_id'].values[0])
            # Insert new rows into tbl_mismatch_record using SQL
            for index, mismatch_record in result['mismatch_record']:
                escaped_mismatch_record = mismatch_record.replace("'", "''")
                short_uuid = str(uuid.uuid4())[:8]
                insert_mismatch_query = f"""
                INSERT INTO tbl_mismatch_record (id, reconcile_id, reason, record_id, fix_status, created_at, updated_at)
                VALUES ('mismatch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{short_uuid}', '{row['reconcile_id']}', '{escaped_mismatch_record}', '{index}', 'open', '{datetime.now()}', '{datetime.now()}');
                """
                cursor.execute(insert_mismatch_query)
            logging.info("END: Insert tbl_mismatch_record for reconcileId: " + preliminary_df['reconcile_id'].values[0])

            logging.info("BEGIN: Inserting tbl_missing_record for reconcileId: " + preliminary_df['reconcile_id'].values[0])
            # Insert new rows into tbl_missing_record using SQL
            for index in result['missing_record']:
                short_uuid = str(uuid.uuid4())[:8]
                insert_missing_query = f"""
                INSERT INTO tbl_missing_record (id, reconcile_id, record_id, fix_status, created_at, updated_at)
                VALUES ('missing_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{short_uuid}', '{row['reconcile_id']}', '{index}', 'open', '{datetime.now()}', '{datetime.now()}');
                """
                cursor.execute(insert_missing_query)
            logging.info("END: Insert tbl_missing_record for reconcileId: " + preliminary_df['reconcile_id'].values[0])
            
            logging.info("BEGIN: Inserting tbl_duplicate_record for reconcileId: " + preliminary_df['reconcile_id'].values[0])
            # Insert new rows into tbl_duplicate_record using SQL
            for index in result['duplicate_record']:
                short_uuid = str(uuid.uuid4())[:8]
                insert_dup_query = f"""
                INSERT INTO tbl_duplicate_record (id, reconcile_id, record_id, fix_status, created_at, updated_at)
                VALUES ('dup_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{short_uuid}', '{row['reconcile_id']}', '{index}', 'open', '{datetime.now()}', '{datetime.now()}');
                """
                cursor.execute(insert_dup_query)
            logging.info("END: Insert tbl_duplicate_record for reconcileId: " + preliminary_df['reconcile_id'].values[0])

            conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()
        logging.info("END: Update data reconciliation result completed successfully for reconcileId: " + preliminary_df['reconcile_id'].values[0])


    except Exception as e:
        logging.error("END: An error occurred during update prelimiary data reconciliation result for reconcileId: "+ preliminary_df['reconcile_id'].values[0], exc_info=True)
        print(f"Error in update_final_result: {e}")