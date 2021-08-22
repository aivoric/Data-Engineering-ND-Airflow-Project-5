from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id,
                data_quality_checks,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        """
        This operator receives a list of dicts containing SQL commands to run
        with expected results. The operator tries to run each SQL and compares
        the result with the expected result. If the two don't match, then a 
        ValueError is raised.
        """
        for quality_check in self.data_quality_checks:
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            name = quality_check.get("name", "UNNANMED QUALITY CHECK")
            sql = quality_check.get("check_sql")
            expected_result = quality_check.get("expected_result")
            
            try:
                self.log.info(f"Launching test. Test name: {name}")
                self.log.info(f"Running query: {sql}")
                result = redshift.get_records(sql)[0][0]
            except Exception as e:
                self.log.info(f"Query failed with exception: {e}")
            
            if result == expected_result:
                self.log.info(f"Test succees! Expected result: {expected_result}, matched with the result: {result}")
            else:
                raise ValueError(f"Data quality check failed. \
                    \nExpected result: {expected_result}.\ Actual result: {result}.")