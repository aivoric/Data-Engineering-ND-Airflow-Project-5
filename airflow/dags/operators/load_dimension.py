from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 insert_mode=False,
                 sql_insert="",
                 sql_drop="",
                 sql_create="",
                 table_name="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.insert_mode = insert_mode
        self.sql_insert = sql_insert
        self.sql_drop = sql_drop
        self.sql_create = sql_create
        self.table_name = table_name

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.insert_mode:
            self.log.info(f"Table drop is enabled. Dropping table {self.table_name}")
            redshift.run(self.sql_drop)
            self.log.info(f"Creating table {self.table_name}")
            redshift.run(self.sql_create)
        self.log.info(f"Inserting data into {self.table_name}.")
        redshift.run(self.sql_insert) 
