import datetime
import pandas as pd
import os
import sys
import psycopg2


def clean_pbp(file_name):
    '''
    Function takes in a dataframe and turns NA to integer 0 to fit with SQL
    database schema

    Inputs:
    df - pandas dataframe

    Outputs:
    df - cleaned pandas dataframe
    '''

    def clean(df):

        col_names = ['coords_x', 'coords_y', 'is_home', 'time_diff',
                     'shot_angle', 'distance', 'event_length', 'game_seconds',
                     'home_corsi', 'away_corsi', 'home_corsi_total',
                     'away_corsi_total']

        for column in col_names:
            df[column] = df[column].fillna(0).astype(int)

        return df

    try:

        # opens nhl pbp csv for importing
        pbp_df = pd.read_csv(file_name, sep='|')

        # clean NA's from integer columns and writes to | delim file
        cleaned_df = clean(pbp_df)

        cleaned_df.to_csv(file_name, sep='|', index=False)

    except Exception:
        print('{} file not found'.format(file_name))


def stats_sql_insert(cursor, connect, database, file_name):
    try:
        with open(file_name, 'r', encoding="utf-8") as pbp:
            sql = ('COPY {} FROM stdin WITH DELIMITER \'|\''
                   'CSV HEADER'.format(database))
            cursor.copy_expert(sql, pbp)
            connect.commit()

    except Exception as ex:
        print(ex)
        print('{} failed to insert'.format(file_name))
        connect.rollback()


def main():
    '''
    Inputs:
    sys.argv[1] - parent directory where folders are located to walk through
    and compile pbp data into one delim file

    Outputs:
    Stats files - total compiled stats in a text file for all the tables in the
    sql database for that season
    '''
    walk_directory = sys.argv[1]

    # create postgresql connection
    conn = psycopg2.connect(os.environ.get('CLOUD_DB_CONNECT'))
    cur = conn.cursor()

    tables = ['masternhlpbp', 'playerstats', 'teamstats', 'playerstats5v5',
              'teamstats5v5', 'playerstatsadj', 'teamstatsadj',
              'playerstatsadj5v5', 'teamstatsadj5v5', 'goaliestats',
              'goaliestats5v5']
    start = datetime.datetime.now()
    folder = datetime.date.today()-datetime.timedelta(days=1)
    folder = folder.isoformat()
    walk_directory = '{}{}/'.format(walk_directory, folder)
    for table in tables:
        if table == 'masternhlpbp':
            clean_pbp('{}{}'.format(walk_directory, 'dailypbp'))
            stats_sql_insert(cur, conn, table, '{}{}'.format(walk_directory,
                                                             'dailypbp'))
        else:
            stats_sql_insert(cur, conn, table, '{}{}{}'.format(walk_directory,
                                                               'daily', table))
    end = datetime.datetime.now()
    print(end - start)


if __name__ == '__main__':
    main()
