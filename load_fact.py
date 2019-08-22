from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    sql_template = """
        INSERT INTO {database}.{table} {sql_statement};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_statement="",
                 database="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.database = database
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('LoadFactOperator running')
        formatted_sql_template=LoadFactOperator.sql_template.format(
          database=self.database,
          table=self.table,
          sql_statement=self.sql_statement
        )
        redshift.run(formatted_sql_template)
        
