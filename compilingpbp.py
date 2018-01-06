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
        df.loc[:,column].fillna(0).astype(int)

    '''
    df['event_index'] = df['event_index'].astype(int)
    df['game_period'] = df['game_period'].astype(int)
    df['home_corsi'] = df['home_corsi'].astype(int)
    '''
    return df

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
    pbp_df = pd.read_csv(file_name, sep = ',')

    #clean NA's from integer columns and writes to | delim file for
    #sql insert
    cleaned_pbp_df = clean_pbp(pbp_df)
    cleaned_pbp_df.to_csv(file_name, sep = '|', index = False)

    '''
    with open(file_name, 'r', encoding = "utf-8") as f:
        # Skip the header row.
        next(f)
        cursor.copy_from(f, 'masternhlpbp', sep='|')
    connect.commit()
    '''

def main():
    '''
    Inputs:
    sys.argv[1] - parent directory where folders are located to walk through
    and compile pbp data into one delim file

    sys.argv[2] - file that will be written to by the script

    Outputs:
    sys.argv[2] - complete pbp file of all NHL games so far in the season
    compiled into a '|' delmited file
    '''
    walk_directory = sys.argv[1]
    comp_pbp_file = sys.argv[2]

    print(walk_directory)
    print(comp_pbp_file)
    #read in pbp file into a pandas dataframe

    #create postgresql connection
    conn = psycopg2.connect("host=localhost dbname=nhl user=matt")
    cur = conn.cursor()
    with open(comp_pbp_file, 'w') as pbp_file:
        x = 0
        for path, subdir, files in os.walk(walk_directory):
            for dirs in subdir:
                try:
                    with open('{}/{}/{}.csv'.format(path, dirs, dirs), 'r') as pbp:
                        header = next(pbp)
                        if x == 0:
                            pbp_file.write(header)
                        pbp_file.writelines(pbp.readlines()[1:])
                except:
                    pass
                x += 1
        sql_insert(comp_pbp_file, cur, conn)



if __name__ == '__main__':
    main()
