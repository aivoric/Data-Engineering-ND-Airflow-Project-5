from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    songplays_upsert_sql = ("""
    CREATE OR REPLACE PROCEDURE songplays_upsert()
    AS $$
    BEGIN
        DROP TABLE IF EXISTS songplays_stage;
    
        CREATE TEMP TABLE songplays_stage (LIKE songplays INCLUDING DEFAULTS); 

        INSERT INTO songplays_stage(
                start_time
                , user_id
                , level
                , song_id
                , artist_id
                , session_id
                , location
                , user_agent) 
                (SELECT
                    se.ts
                    , se.user_id
                    , se.level
                    , ss.song_id
                    , ss.artist_id
                    , se.session_id
                    , se.location
                    , se.user_agent
                FROM staging_events se
                LEFT JOIN {} ss ON ss.title = se.song
                WHERE page = 'NextSong'
                );

        DELETE FROM songplays  
        USING songplays_stage 
        WHERE songplays.session_id = songplays_stage.session_id
        AND songplays.start_time = songplays_stage.start_time;
        
        INSERT INTO songplays (
                start_time
                , user_id
                , level
                , song_id
                , artist_id
                , session_id
                , location
                , user_agent) 
                (SELECT
                    ss.start_time
                    , ss.user_id
                    , ss.level
                    , ss.song_id
                    , ss.artist_id
                    , ss.session_id
                    , ss.location
                    , ss.user_agent
                FROM songplays_stage ss
                );
        
        DROP TABLE songplays_stage;
    END;
    $$ LANGUAGE plpgsql;
""")

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 read_from_table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.read_from_table = read_from_table

    def execute(self, context):
        """
        This operator is responsible for the insert data into the fact table.
        
        The fact table is not dropped when an insert (upsert) is performed into it. 
        Since Redshift is a columnar database, you need to perform a merge operation which is
        documented here:
        https://docs.aws.amazon.com/redshift/latest/dg/merge-specify-a-column-list.html
        
        To simplify the merge process, this Operator does the following:
        1. Creates a merge procedure in Redshift which is provided as part of the operator
        2. Runs the procedure which performs an UPSERT into Redshift
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Upserting into songplays.")
        
        formatted_sql = LoadFactOperator.songplays_upsert_sql.format(
            self.read_from_table
        )
        redshift.run(formatted_sql) # Create the stored procedure
        redshift.run("CALL songplays_upsert();") # Run the stored procedure
