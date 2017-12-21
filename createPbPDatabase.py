import psycopg2

conn = psycopg2.connect("host=localhost dbname=nhl user=matt")

cursor = conn.cursor()

cursor.execute("""
        CREATE TABLE masternhlpbp(
        event_index integer,
        season text,
        game_id text,
        game_date date,
        session text,
        game_period integer,
        game_seconds integer,
        event_type text,
        event_description text,
        event_detail text,
        event_team text,
        event_player_1 text,
        event_player_2 text,
        event_player_3 text,
        event_length integer,
        coords_x integer,
        coords_y integer,
        players_substituted text,
        home_on_1 text,
        home_on_2 text,
        home_on_3 text,
        home_on_4 text,
        home_on_5 text,
        home_on_6 text,
        away_on_1 text,
        away_on_2 text,
        away_on_3 text,
        away_on_4 text,
        away_on_5 text,
        away_on_6 text,
        home_goalie text,
        away_goalie text,
        home_team text,
        away_team text,
        home_skaters text,
        away_skaters text,
        home_score text,
        away_score text,
        game_score_state text,
        game_strength_state text,
        highlight_code text,
        is_home integer,
        time_diff integer,
        home_corsi integer,
        away_corsi integer,
        home_corsi_total integer,
        away_corsi_total integer,
        is_rebound integer,
        is_rush integer,
        shot_angle real,
        distance real,
        shooter_strength text,
        xG real,
        home_xG real,
        away_xG real,
        home_5v5_xG real,
        away_5v5_xG real,
        run_home_xg real,
        run_away_xg real,
        run_home_5v5_xg real,
        run_away_5v5_xg real)

"""
)

conn.commit()

