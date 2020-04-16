from datetime import datetime, timedelta
import os
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator, LoadFactOperator,LoadDimensionOperator, DataQualityOperator
from airflow.operators import GenerateDDLOperator
from helpers import SqlQueries
from helpers import TableQueries
from airflow.operators.python_operator import PythonOperator

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'mustafa',
    'start_date': datetime(2018,11,9),
    'retries': 3,
    'email_on_retry': False,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          catchup = True,
          max_active_runs=1,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)



run_ddl = GenerateDDLOperator(
    task_id='execute_ddl',
    dag=dag,
    redshift_conn_id="redshift",
    sql_statement = TableQueries.create_table_queries
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table_name = "staging_songs",
    aws_credentials_id='aws_credentials',
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="song_data"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table_name = "staging_events",
    aws_credentials_id='aws_credentials',
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_statement = SqlQueries.songplay_table_insert    
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_statement = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_statement = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_statement = SqlQueries.time_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_statement = SqlQueries.song_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables = ['songs','users','artists','songplays'],
    record_count=1
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> run_ddl >> [stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table 
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator
