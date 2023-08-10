# main.py
from config_loader import config
from modules.extract_data import mysql_query, postgres_query, bq_query
from modules.data_reconciler import (
    reconcile_mysql_with_bigquery,
    reconcile_postgres_with_bigquery,
    store_preliminary_result,
    update_final_result,
)

def reconcile_data_with_bigquery():
    preliminary_result = store_preliminary_result()

    #test
    #columns_to_drop_postgres = ['json_col', 'jsonb_col','array_col','inet_col','point_col']
    columns_to_drop_bq = ['__deleted', 'key']
    
    if config['src_type'] == 'mysql':
        mysql_df = mysql_query("SELECT * FROM {} LIMIT 10".format(config['src_table']), config['mysql'])
        bq_df = bq_query("SELECT * FROM {}.{} LIMIT 10".format(config['target_dataset'], config['target_table']), config['src_project_id'])
        result = reconcile_mysql_with_bigquery(mysql_df, bq_df)
        update_final_result(preliminary_result, result)
    elif config['src_type'] == 'postgres':
        postgres_df = postgres_query("SELECT * FROM {}.{} ORDER BY id".format(config['postgres']['schema'], config['src_table']), config['postgres'])
        bq_df = bq_query("SELECT * FROM {}.{} ORDER BY id".format(config['target_dataset'], config['target_table']), config['src_project_id'])

       #postgres_df.drop(columns_to_drop_postgres, axis=1, inplace=True)
        bq_df.drop(columns_to_drop_bq, axis=1, inplace=True)

        print(postgres_df.timestamp_col)
        print(bq_df.timestamp_col)

        result = reconcile_postgres_with_bigquery(postgres_df, bq_df)
        update_final_result(preliminary_result, result)
    else:
        raise ValueError("Invalid src_type. Supported values are 'mysql' and 'postgres'.")

def main():
    # Perform the data reconciliation between MySQL or PostgreSQL and BigQuery
    reconcile_data_with_bigquery()

if __name__ == "__main__":
    main()
