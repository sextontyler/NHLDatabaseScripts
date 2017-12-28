import psycopg2
import pandas as pd
import datetime
import sys

def write_top5(top5_file, sql_query, date):
    '''
    Inputs:
    top5_file - the file name to write to
    sql_query - the results of the sql query to return the top 5 ixG players

    Outputs:
    None
    '''
    with open(top5_file, 'w+') as top5:
        top5.write('{} ixG Leaders:\n'.format(date))
        for row in sql_query:
            top5.write('{}\n'.format(str(row).replace('(', '').replace(',', ' ').replace(')', '').replace("'", '')))
def main():
    '''
    This function takes in a text file and writes the results of an sql query
    to it.

    inputs:
    sys.argv[1] - text file that holds the top 5 ixG performers from yesterday
    for keylesstwitterpost.py script to read in and post

    Outputs:
    sys.argv[1] - updated text file with the old contents replaced by the new
    results from the sql query

    None
    '''
    #Takes in file name
    top5 = sys.argv[1]

    #Gets yesterday's date and reformats it to the same as database
    date = datetime.datetime.now() - datetime.timedelta(days = 1)
    sql_date = date.strftime('%Y-%m-%d')

    #opens connection to postgres db, executes query, and store query in a list
    conn = psycopg2.connect("host=localhost dbname=nhl user=matt")
    cur = conn.cursor()
    top5_query = "SELECT event_player_1, event_team, sum(xg) AS ixG FROM\
     masternhlpbp WHERE game_date = '{}' GROUP BY event_player_1, event_team\
     ORDER BY SUM(xg) DESC LIMIT 5;".format(sql_date)
    cur.execute(top5_query)
    rows = cur.fetchall()

    #writes sql query to file
    write_top5(top5, rows, sql_date)

if __name__ == '__main__':
    main()

