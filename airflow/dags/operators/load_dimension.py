from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    
    sql_truncate = "TRUNCATE {}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 insert_mode=False,
                 sql_create="",
                 table_name="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.insert_mode = insert_mode
        self.sql_create = sql_create
        self.table_name = table_name

    def execute(self, context):
        """
        This operator is used to query a Redshift table using the provided sql_insert
        statement, and then insert the results to a dimension table provided in table_name.
        
        If insert_mode is set to False, then the table is first truncated before the insert
        statement is run. See: https://docs.aws.amazon.com/redshift/latest/dg/r_TRUNCATE.html
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.insert_mode:
            self.log.info(f"Truncating {self.table_name}.")
            redshift.run(self.sql_truncate.format(self.table_name))
        self.log.info(f"Inserting data into {self.table_name}.")
        redshift.run(self.sql_insert) 
