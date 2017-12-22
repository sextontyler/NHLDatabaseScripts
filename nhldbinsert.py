import psycopg2
import pandas as pd
import sys

def clean_pbp(df):
    col_names = ['coords_x', 'coords_y', 'is_home', 'time_diff', 'shot_angle',
            'distance', 'event_length', 'game_seconds']

    for column in col_names:
        df[column] = df[column].fillna(0).astype(int)

    return df


def sql_insert(file_name, cursor, connect):
    #opens nhl pbp csv for importing
    with open(file_name, 'r') as f:
        # Skip the header row.
        next(f)
        cursor.copy_from(f, 'masternhlpbp', sep='|')
    connect.commit()

def main():
    #read in pbp file into a pandas dataframe
    daily_pbp = sys.argv[1]
    pbp_df = pd.read_csv(daily_pbp, sep = '|')

    #create postgresql connection
    conn = psycopg2.connect("host=localhost dbname=nhl user=matt")
    cur = conn.cursor()

    #clean NA's from integer columns
    cleaned_pbp_df = clean_pbp(pbp_df)
    cleaned_pbp_df.to_csv(daily_pbp, sep = '|', index = False)

    #perform sql insert
    sql_insert(daily_pbp, cur, conn)
    conn.commit()

if __name__ == "__main__":
    main()
