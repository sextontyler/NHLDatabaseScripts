import psycopg2
import pandas as pd
import datetime
import sys

def write_top5(top5_file, sql_query, date):
    '''
    inputs:
    top5_file - the file name to write to
    sql_query - the results of the sql query to return the top 5 ixG players

    returns:

    None
    '''
    with open(top5_file, 'w+') as top5:
        top5.write('{} ixG Leaders:\n'.format(date))
        for row in sql_query:
            top5.write('{}\n'.format(str(row)))
def main():
    '''
    This function takes in a text file and writes the results of an sql query
    to it.

    inputs:
    command line argument passes the script a file

    returns:

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
    print(top5_query)
    cur.execute(top5_query)
    rows = cur.fetchall()
    write_top5(top5, rows, date)

if __name__ == '__main__':
    main()

