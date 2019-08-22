import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table_column = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_column = table_column

    def execute(self, context):
        self.log.info('DataQualityOperator running')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.table_column:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table[0]}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table[0]} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table[0]} contained 0 rows")
            null_values = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table[0]} WHERE {table[1]} IS NULL")
            if null_values[0][0] > 0:
                raise ValueError(f"Data quality check failed.  {table[0]} {table[1]} contains null values")
            logging.info(f"Data quality on table {table[0]} check passed with {records[0][0]} records")

                