from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables = "",
                 record_count="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables= tables
        self.record_count=record_count

    def execute(self, context):
        self.log.info('Data Quality Check')
        self.log.info("Creating Redshift connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Running Quality checks on the tables {}".format(self.tables))
        for table in self.table:
            records = redshift.get_records(f"{self.sql_check} {table}")
            total_records=records[0][0]
            if len(records) < self.record_count or len(records[0]) < self.record_count:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            elif num_records < self.record_count:
                raise ValueError(f"Data quality check failed. {table} contains {total_records} row")
            else:
                self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")