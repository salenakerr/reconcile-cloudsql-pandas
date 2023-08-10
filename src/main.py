import logging,os
from urllib.parse import quote_plus
from config_loader import config, get_google_secret
from modules.extract_data import mysql_query, postgres_query, bq_query
from modules.data_reconciler import (
    reconcile_mysql_with_bigquery,
    reconcile_postgres_with_bigquery,
)
from modules.store_result import (
    store_preliminary_result,
    update_final_result,
)

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_data(query_string, source_type, project_id=None):
    if source_type == 'mysql':
        return mysql_query(query_string, config['mysql'])
    elif source_type == 'postgres':
        return postgres_query(query_string, config['postgres'])
    elif source_type == 'bigquery':
        return bq_query(query_string, project_id)
    else:
        raise ValueError("Invalid source_type. Supported values are 'mysql', 'postgres', and 'bigquery'.")

def reconcile_data_with_bigquery():
    try:
        logging.info("BEGIN: Starting data reconciliation process...")

        preliminary_result = store_preliminary_result()

        logging.info("BEGIN: Fetching and preprocessing data...")

        columns_to_drop_bq = ['__deleted', 'key']
        # Fetching and preprocessing data
        src_query_string = "SELECT * FROM {}".format(config['src_table'])
        target_query_string = "SELECT * FROM {}.{}".format(config['target_dataset'], config['target_table'])
        
        if 'primary_key' in config:
            src_query_string += " ORDER BY {}".format(config['primary_key'])
            target_query_string += " ORDER BY {}".format(config['primary_key'])
        
        src_df = fetch_data(src_query_string, config['src_type'])
        target_df = fetch_data(target_query_string, 'bigquery', config['src_project_id'])
        target_df.drop(columns_to_drop_bq, axis=1, inplace=True)

        logging.info("END: Fetching and preprocessing data completed successfully.")


        #"Performing data reconciliation
        if config['src_type'] == 'mysql':
            result = reconcile_mysql_with_bigquery(src_df, target_df)
        elif config['src_type'] == 'postgres':
            result = reconcile_postgres_with_bigquery(src_df, target_df)
        else:
            raise ValueError("Invalid src_type. Supported values are 'mysql' and 'postgres'.")

        update_final_result(preliminary_result, result)

        logging.info("END: Data reconciliation process completed successfully.")
    except Exception as e:
        logging.error("END: An error occurred during data reconciliation:", exc_info=True)
        print("An error occurred:", str(e))

def main():
    try:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['default_sa_path']
        # Perform the data reconciliation between MySQL/PostgreSQL and BigQuery
        reconcile_data_with_bigquery()
    except Exception as e:
        logging.error("END: An error occurred:", exc_info=True)
        print("An error occurred:", str(e))

if __name__ == "__main__":
    main()
