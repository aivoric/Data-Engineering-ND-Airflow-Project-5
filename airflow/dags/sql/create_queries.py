user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id                 INTEGER                 NOT NULL
        , first_name            VARCHAR(50)             NOT NULL
        , last_name             VARCHAR(100)            NOT NULL
        , gender                VARCHAR(1)              NOT NULL
        , level                 VARCHAR(10)             NOT NULL
        , primary key(user_id)
    )
    diststyle auto;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id                 VARCHAR(30)             NOT NULL
        , title                 VARCHAR(300)            NOT NULL
        , artist_id             VARCHAR(30)             NOT NULL
        , year                  SMALLINT                NOT NULL
        , duration              FLOAT                   NOT NULL
        , primary key(song_id)
    )
    diststyle auto;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id               VARCHAR(30)             NOT NULL
        , name                  VARCHAR(300)            NOT NULL
        , location              VARCHAR(200)
        , latitude              FLOAT
        , longitude             FLOAT
        , primary key(artist_id)
    )
    diststyle auto;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time              BIGINT                  NOT NULL
        , hour                  SMALLINT                NOT NULL
        , day                   SMALLINT                NOT NULL
        , week                  SMALLINT                NOT NULL
        , month                 SMALLINT                NOT NULL
        , year                  SMALLINT                NOT NULL
        , weekday               SMALLINT                NOT NULL
        , primary key(start_time)
    )
    diststyle auto;
""")