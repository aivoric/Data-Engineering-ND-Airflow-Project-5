from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from sql.insert_queries import user_table_insert, song_table_insert,\
    artist_table_insert, time_table_insert
from sql.drop_queries import user_table_drop, song_table_drop,\
    artist_table_drop, time_table_drop
from sql.create_queries import user_table_create, song_table_create,\
    artist_table_create, time_table_create

default_args = {
    'owner': 'sparkify',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG('sparkify',
          default_args=default_args,
          start_date=datetime(2019, 1, 12),
          end_date=datetime(2019, 1, 15),
          schedule_interval='@daily',
          description='Load and transform data in Redshift with Airflow',
          catchup=True,
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_role_arn="arn:aws:iam::582160271005:role/redshift_role",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_role_arn="arn:aws:iam::582160271005:role/redshift_role",
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
    sql_drop=user_table_drop,
    sql_create=user_table_create,
    table_name="users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    insert_mode=False,
    sql_insert=song_table_insert,
    sql_drop=song_table_drop,
    sql_create=song_table_create,
    table_name="songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    insert_mode=False,
    sql_insert=artist_table_insert,
    sql_drop=artist_table_drop,
    sql_create=artist_table_create,
    table_name="artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    insert_mode=False,
    sql_insert=time_table_insert,
    sql_drop=time_table_drop,
    sql_create=time_table_create,
    table_name="time"
)

check_song_staging_not_empty = DataQualityOperator(
    task_id='check_song_staging_not_empty',
    dag=dag,
    test_description="Test to check that the table is not empty.",
    redshift_conn_id="redshift",
    sql="SELECT COUNT(*) FROM staging_songs",
    expected_result="not_empty",
)

check_events_staging_not_empty = DataQualityOperator(
    task_id='check_events_staging_not_empty',
    dag=dag,
    test_description="Test to check that the table is not empty.",
    redshift_conn_id="redshift",
    sql="SELECT COUNT(*) FROM staging_events",
    expected_result="not_empty",
)

check_user_table_not_empty = DataQualityOperator(
    task_id='check_user_table_not_empty',
    dag=dag,
    test_description="Test to check that the table is not empty.",
    redshift_conn_id="redshift",
    sql="SELECT COUNT(*) FROM users",
    expected_result="not_empty",
)

check_song_table_not_empty = DataQualityOperator(
    task_id='check_song_table_not_empty',
    dag=dag,
    test_description="Test to check that the table is not empty.",
    redshift_conn_id="redshift",
    sql="SELECT COUNT(*) FROM songs",
    expected_result="not_empty",
)

check_artist_table_not_empty = DataQualityOperator(
    task_id='check_artist_table_not_empty',
    dag=dag,
    test_description="Test to check that the table is not empty.",
    redshift_conn_id="redshift",
    sql="SELECT COUNT(*) FROM artists",
    expected_result="not_empty",
)

check_time_table_not_empty = DataQualityOperator(
    task_id='check_time_table_not_empty',
    dag=dag,
    test_description="Test to check that the table is not empty.",
    redshift_conn_id="redshift",
    sql="SELECT COUNT(*) FROM time",
    expected_result="not_empty",
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift >> check_song_staging_not_empty
start_operator >> stage_songs_to_redshift >> check_events_staging_not_empty
check_song_staging_not_empty >> load_songplays_table
check_events_staging_not_empty >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> check_user_table_not_empty >> end_operator
load_song_dimension_table >> check_song_table_not_empty >> end_operator
load_artist_dimension_table >> check_artist_table_not_empty >> end_operator
load_time_dimension_table >> check_time_table_not_empty >> end_operator