# main.py
from config_loader import config
from modules.extract_data import (
    mysql_query,
    postgres_query,
    bq_query
)
from modules.data_reconciler import (
    reconcile_mysql_data,
    reconcile_postgres_data,
    store_preliminary_result,
    update_final_result,
)

def reconcile_data_with_bigquery():
    if config['src_type'] == 'mysql':
        mysql_result = store_preliminary_result(mysql_result, db_config['result_postgres'])
        mysql_df = mysql_query("SELECT * FROM {}".format(config['src_table']))
        bq_df = bq_query("SELECT * FROM {}{}".format(config['bq_dataset']).format(config['bq_table']))
        mysql_result = reconcile_mysql_data(mysql_df, bq_df)
        update_final_result(mysql_result, db_config['result_postgres'])
    elif config['src_type'] == 'postgres':
        postgres_result = store_preliminary_result(postgres_result, db_config['result_postgres'])
        postgres_df = postgres_query("SELECT * FROM {}{}".format(config['postgres']['schema']).format(config['src_table']))
        bq_df = bq_query("SELECT * FROM {}{}".format(config['bq_dataset']).format(config['bq_table']))
        postgres_result = reconcile_postgres_data(postgres_df, bq_df)
        update_final_result(postgres_result, db_config['result_postgres'])
    else:
        raise ValueError("Invalid src_type. Supported values are 'mysql' and 'postgres'.")

def main():
    # Perform the data reconciliation between MySQL or PostgreSQL and BigQuery
    reconcile_data_with_bigquery()

if __name__ == "__main__":
    main()
