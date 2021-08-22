from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from sql.insert_queries import user_table_insert, song_table_insert,\
    artist_table_insert, time_table_insert

default_args = {
    'owner': 'sparkify',
    'depends_on_past': False,
    'retries': 3,
    #'retry_delay': timedelta(minutes=5),
    'retry_delay': timedelta(seconds=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG('sparkify',
          default_args=default_args,
          start_date=datetime(2019, 1, 12),
          end_date=datetime(2019, 1, 15),
          schedule_interval='@hourly',
          description='Load and transform data in Redshift with Airflow',
          catchup=True,
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_role_arn="arn:aws:iam::300259718964:role/RedshiftS3ReadOnly",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_role_arn="arn:aws:iam::300259718964:role/RedshiftS3ReadOnly",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    read_from_table="staging_songs",
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    insert_mode=False,
    sql_insert=user_table_insert,
    table_name="users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    insert_mode=False,
    sql_insert=song_table_insert,
    table_name="songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    insert_mode=False,
    sql_insert=artist_table_insert,
    table_name="artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    insert_mode=False,
    sql_insert=time_table_insert,
    table_name="time"
)

data_quality_check = DataQualityOperator(
    task_id='Run_data_quality_check',
    dag=dag,
    redshift_conn_id="redshift",
    data_quality_checks = [
            {'name': 'Users table not null values.', 'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
            {'name': 'Artists table no null values.', 'check_sql': "SELECT COUNT(*) FROM artists WHERE name IS NULL", 'expected_result': 0},
            {'name': 'Songs table not null values.', 'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0},
            {'name': 'No weekdays with null.', 'check_sql': "SELECT COUNT(*) FROM time WHERE weekday IS NULL", 'expected_result': 0},
            {'name': 'No users without a name.', 'check_sql': "SELECT COUNT(*) FROM users WHERE first_name IS NULL", 'expected_result': 0},
        ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> data_quality_check
data_quality_check >> end_operator