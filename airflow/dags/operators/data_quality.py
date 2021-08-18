from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                test_description,
                redshift_conn_id,
                sql,
                expected_result,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_description = test_description
        self.sql = sql
        self.expected_result = expected_result

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Launching test. Test description: {self.test_description}")
        result = redshift.get_records(self.sql)
        self.log.info(f"Retrieved result from query: {result}.")
        if self.expected_result == "not_empty":
            if len(result) < 1 or len(result[0]) < 1:
                raise ValueError(f"Data quality check failed. \
                    \nExpected result: {self.expected_result}.\ Actual result: {result}.")