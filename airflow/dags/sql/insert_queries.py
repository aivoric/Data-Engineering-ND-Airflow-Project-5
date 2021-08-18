user_table_insert = ("""
    INSERT INTO users(
        SELECT
            a.user_id
            , a.first_name
            , a.last_name
            , a.gender
            , a.level
        FROM staging_events a
        INNER JOIN (
            SELECT user_id, MAX(ts) as ts
            FROM staging_events
            GROUP BY user_id
        ) b ON a.user_id = b.user_id AND a.ts = b.ts
        )
""")

song_table_insert = ("""
    INSERT INTO songs(
        SELECT
            DISTINCT ss.song_id
            , ss.title
            , ss.artist_id
            , ss.year
            , ss.duration
        FROM staging_songs ss
        WHERE song_id IS NOT NULL
        )
""")

artist_table_insert = ("""
    INSERT INTO artists(
        SELECT
            DISTINCT ss.artist_id
            , artist_name AS name
            , artist_location AS location
            , artist_latitude AS latitude
            , artist_longitude AS longtitude
        FROM staging_songs ss
        WHERE artist_id IS NOT NULL
        )
""")

time_table_insert = ("""
    INSERT INTO time(
        SELECT
            DISTINCT ts AS start_time
            , EXTRACT(hour FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS hour
            , EXTRACT(day FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS day
            , EXTRACT(week FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS week
            , EXTRACT(month FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS month
            , EXTRACT(year FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS year
            , EXTRACT(weekday FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS weekday
        FROM staging_events se
        )
""")

