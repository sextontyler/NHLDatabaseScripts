import psycopg2

def drop_tables(connection):
    '''
    Function to drop all tables in the NHL database to prepare for
    reinsertion.
    Inputs:
    connection - Connection to the PostgreSQL database

    Outputs:
    None
    '''
    #List of tables in the NHL database
    tables = ['masternhlpbp', 'playerstats', 'teamstats', 'playerstats5v5',
            'teamstats5v5', 'playerstatsadj', 'teamstatsadj', 'playerstatsadj5v5',
            'teamstatsadj5v5']

    for table in tables:
        try:
            connection.execute('DROP TABLE {}'.format(table))
        except:
            print("Table does not exit")
            pass

    connection.commit()


def create_tables(connection):
    '''
    Function to create all the tables needed for the NHL SQL database

    Inputs:
    connection - Connection to the PostgreSQL database

    Outputs:
    None
    '''

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

    connection.execute("""
    CREATE TABLE teamstats5v5(
        team text,
        game_id text,
        game_date date,
        TOI real,
        CF real,
        CA real,
        C+/- real,
        FF real,
        FA real,
        xGF real,
        xGA real,
        GF integer,
        GA integer,
        CF60 real,
        CA60 real,
        CF% real,
        FF60 real,
        FA60 real,
        FF% real,
        GF% real,
        xGF60 real,
        xGA60 real,
        xGF% real,
        GF60 real,
        GA60 real,
        is_home int
        )
    """
    )
    connection.execute("""
    CREATE TABLE teamstats(
        team text,
        game_id text,
        game_date date,
        TOI real,
        CF real,
        CA real,
        C+/- real,
        FF real,
        FA real,
        xGF real,
        xGA real,
        GF integer,
        GA integer,
        CF60 real,
        CA60 real,
        CF% real,
        FF60 real,
        FA60 real,
        FF% real,
        GF% real,
        xGF60 real,
        xGA60 real,
        xGF% real,
        GF60 real,
        GA60 real,
        is_home int
        )
    """
    )
    connection.execute("""
    CREATE TABLE teamstatsadj(
        team text,
        game_id text,
        game_date date,
        TOI real,
        CF real,
        CA real,
        C+/- real,
        FF real,
        FA real,
        xGF real,
        xGA real,
        GF integer,
        GA integer,
        CF60 real,
        CA60 real,
        CF% real,
        FF60 real,
        FA60 real,
        FF% real,
        GF% real,
        xGF60 real,
        xGA60 real,
        xGF% real,
        GF60 real,
        GA60 real,
        is_home int
        )
    """
    )

    connection.execute("""
    CREATE TABLE teamstatsadj5v5(
        team text,
        game_id text,
        game_date date,
        TOI real,
        CF real,
        CA real,
        C+/- real,
        FF real,
        FA real,
        xGF real,
        xGA real,
        GF integer,
        GA integer,
        CF60 real,
        CA60 real,
        CF% real,
        FF60 real,
        FA60 real,
        FF% real,
        GF% real,
        xGF60 real,
        xGA60 real,
        xGF% real,
        GF60 real,
        GA60 real,
        is_home int
        )
    """
    )

    connection.execute("""
    CREATE TABLE playerstatsadj5v5(
        player text
        team text,
        TOI real,
        CF real,
        CA real,
        C+/- real,
        FF real,
        FA real,
        xGF real,
        xGA real,
        GF integer,
        GA integer,
        G integer,
        A1 integer,
        A2 integer,
        ixG real,
        CF60 real,
        CA60 real,
        CF% real,
        FF60 real,
        FA60 real,
        FF% real,
        GF60 real,
        GA60 real,
        GF% real,
        xGF60 real,
        xGA60 real,
        xGF% real,
        G60 real,
        A160 real,
        A260 real,
        P60 real,
        ixG60 real,
        game_id text,
        game_date date
        )
    """
    connection.execute("""
    CREATE TABLE playerstatsadj(
        player text
        team text,
        TOI real,
        CF real,
        CA real,
        C+/- real,
        FF real,
        FA real,
        xGF real,
        xGA real,
        GF integer,
        GA integer,
        G integer,
        A1 integer,
        A2 integer,
        ixG real,
        CF60 real,
        CA60 real,
        CF% real,
        FF60 real,
        FA60 real,
        FF% real,
        GF60 real,
        GA60 real,
        GF% real,
        xGF60 real,
        xGA60 real,
        xGF% real,
        G60 real,
        A160 real,
        A260 real,
        P60 real,
        ixG60 real,
        game_id text,
        game_date date
        )
    """
    connection.execute("""
    CREATE TABLE playerstats5v5(
        player text
        team text,
        TOI real,
        CF real,
        CA real,
        C+/- real,
        FF real,
        FA real,
        xGF real,
        xGA real,
        GF integer,
        GA integer,
        G integer,
        A1 integer,
        A2 integer,
        ixG real,
        CF60 real,
        CA60 real,
        CF% real,
        FF60 real,
        FA60 real,
        FF% real,
        GF60 real,
        GA60 real,
        GF% real,
        xGF60 real,
        xGA60 real,
        xGF% real,
        G60 real,
        A160 real,
        A260 real,
        P60 real,
        ixG60 real,
        game_id text,
        game_date date
        )
    """
    connection.execute("""
    CREATE TABLE playerstats(
        player text
        team text,
        TOI real,
        CF real,
        CA real,
        C+/- real,
        FF real,
        FA real,
        xGF real,
        xGA real,
        GF integer,
        GA integer,
        G integer,
        A1 integer,
        A2 integer,
        ixG real,
        CF60 real,
        CA60 real,
        CF% real,
        FF60 real,
        FA60 real,
        FF% real,
        GF60 real,
        GA60 real,
        GF% real,
        xGF60 real,
        xGA60 real,
        xGF% real,
        G60 real,
        A160 real,
        A260 real,
        P60 real,
        ixG60 real,
        game_id text,
        game_date date
        )
    """
    )

def main():
    '''
    Function to drop all tables in the existing database and create new ones
    for insertion of stats to setup the database fresh
    Inputs:
    None

    Outputs:
    None
    '''
    conn = psycopg2.connect("host=localhost dbname=nhl user=matt")
    cursor = conn.cursor()
    drop_tables(cursor)
    create_tables(cursor)
    conn.commit()

if __name__ == '__main__':
    main()

