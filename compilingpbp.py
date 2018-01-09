import os
import pandas as pd
import sys
import psycopg2

def clean_pbp(df):
    '''
    Function takes in a dataframe and turns NA to integer 0 to fit with SQL
    database schema

    Inputs:
    df - pandas dataframe

    Outputs:
    df - cleaned pandas dataframe
    '''
    df = df[df['session']!='session']
    col_names = ['coords_x', 'coords_y', 'is_home', 'time_diff',
             'event_length', 'game_seconds', 'event_index',
            'game_period', 'home_corsi', 'away_corsi', 'home_corsi_total',
            'away_corsi_total', 'is_rebound', 'is_rush']

    for column in col_names:
        df[column] = df[column].fillna(0).astype(int)

    '''
    df['event_index'] = df['event_index'].astype(int)
    df['game_period'] = df['game_period'].astype(int)
    df['home_corsi'] = df['home_corsi'].astype(int)
    '''
    return df

def stats_compile(cursor, connect, database, directory):
    for path, subdir, files in os.walk(directory):
        for dirs in subdir:
            try:
                if database == 'masternhlpbp':
                    with open('{}/{}/{}'.format(path, dirs, dirs), 'r', encoding = "utf-8") as pbp:
                        #pbp_df = pd.read_csv(file_name, sep = '|')
                        #cleaned_pbp_df = clean_pbp(pbp_df)
                        #cleaned_pbp_df.to_csv(file_name, sep = '|')
                        sql = "COPY {} FROM stdin WITH DELIMITER '|' CSV HEADER".format(database)
                        cursor.copy_expert(sql, pbp)
                        connect.commit()
                else:
                    with open('{}/{}/{} {}'.format(path, dirs, dirs, database), 'r', encoding = "utf-8") as pbp:
                        sql = "COPY {} FROM stdin WITH DELIMITER '|' CSV HEADER".format(database)
                        cursor.copy_expert(sql, pbp)
                        connect.commit()

            except:
                pass

def main():
    '''
    Inputs:
    sys.argv[1] - parent directory where folders are located to walk through
    and compile pbp data into one delim file

    Outputs:
    None
    '''
    walk_directory = sys.argv[1]
    files_directory = sys.argv[2]

    #create postgresql connection
    conn = psycopg2.connect("host=localhost dbname=nhl user=matt")
    cur = conn.cursor()

    tables = ['playerstats', 'teamstats', 'playerstats5v5',
            'teamstats5v5', 'playerstatsadj', 'teamstatsadj', 'playerstatsadj5v5',
            'teamstatsadj5v5']

    for table in tables:
        stats_compile(cur, conn, table, walk_directory)


if __name__ == '__main__':
    main()
