from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GenerateDDLOperator(BaseOperator):
    ui_color = '#353210'

    @apply_defaults
    def __init__(self,
                     redshift_conn_id="",
                     sql_statement="",
                 *args, **kwargs):

        super(GenerateDDLOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_statement=sql_statement


    def execute(self, context):
        self.log.info("Creating Redshift connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Redshift connection Check: {redshift}")
        self.log.info(f"SQL statement : {self.sql_statement}")
        redshift.run(self.sql_statement)




