import psycopg2
import pandas as pd
import sys

def clean_pbp(df):
    '''
    Function takes in a dataframe and turns NA to integer 0 to fit with SQL
    database schema

    Inputs:
    df - pandas dataframe

    Outputs:
    df - cleaned pandas dataframe
    '''
    col_names = ['coords_x', 'coords_y', 'is_home', 'time_diff', 'shot_angle',
            'distance', 'event_length', 'game_seconds']

    for column in col_names:
        df[column] = df[column].fillna(0).astype(int)

    return df


def games_played_check(file_name):
    '''
    Function checks the pbp text file in sys args to see if there is actually
    data to input into the SQL database

    Inputs:
    file_name - string of file path for data

    Ouputs:
    boolean - True or False whether string is present in file indicating if
    games were played
    '''
    with open(file_name, 'r') as f:
        first_line = str(next(f)).strip()
        if first_line == 'No games today':
            return False
        else:
            return True

def sql_insert(file_name, cursor, connect):
    '''
    Function takes cleaned data and inserts it into the Postgres SQL database

    Inputs:
    file_name - (string) file path of | delimited data
    cursor    - (object) Postgres connection cursor
    connect   - (object) Postgres connection

    Outputs:
    None
    '''

    #opens nhl pbp csv for importing
    pbp_df = pd.read_csv(file_name, sep = '|')

    print(file_name)
    print(type(file_name))

    #clean NA's from integer columns and writes to | delim file for
    #sql insert
    cleaned_pbp_df = clean_pbp(pbp_df)
    cleaned_pbp_df.to_csv(file_name, sep = '|', index = False)

    with open(file_name, 'r') as f:
        # Skip the header row.
        next(f)
        cursor.copy_from(f, 'masternhlpbp', sep='|')
    connect.commit()

def main():
    '''
    This script takes the output of my daily R NHL scraping script and cleans
    it and inserts into the masternhlpbp db on the Postgres server.  If there
    were no games played it simply returns none and ceases execution

    Inputs:
    sys.argv[1] - the daily NHL PbP output from the R scraper

    Outputs:
    None
    '''

    #read in pbp file into a pandas dataframe
    daily_pbp = sys.argv[1]

    #create postgresql connection
    conn = psycopg2.connect("host=localhost dbname=test user=matt")
    cur = conn.cursor()


    if games_played_check(daily_pbp):
        #perform sql insert
        sql_insert(daily_pbp, cur, conn)
        conn.commit()
    else:
        print("No games today")
        return None

if __name__ == "__main__":
    main()
