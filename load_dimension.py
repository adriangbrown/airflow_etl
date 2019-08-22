from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#40E0D0'

    trunc_template = """
        TRUNCATE {database}.{table};
    """
    sql_template = """
        INSERT INTO {database}.{table} {sql_statement};
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_statement="",
                 database="",
                 trunc_table="",
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.database = database
        self.trunc_table = trunc_table
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('LoadDimensionOperator running')
        formatted_sql_template=LoadDimensionOperator.sql_template.format(
          database=self.database,
          table=self.table,
          trunc_table=self.trunc_table,
          sql_statement=self.sql_statement
        )
        formatted_trunc_template=LoadDimensionOperator.trunc_template.format(
          database=self.database,
          table=self.table,
          trunc_table=self.trunc_table,
          sql_statement=self.sql_statement
        )
        if self.trunc_table == '':
            redshift.run(formatted_sql_template)
        else:
            redshift.run(formatted_trunc_template)
            redshift.run(formatted_sql_template)