import psycopg2

conn = psycopg2.connect("host=localhost dbname=nhl user=matt")
cur = conn.cursor()

file_name = '/Users/MattBarlowe/HockeyStuff/CompleteNHLPbPData/\
2018PbPDataGames20516-20525.csv'
#opens nhl pbp csv for importing
with open(file_name, 'r') as f:
    # Skip the header row.
    next(f)
    cur.copy_from(f, 'masternhlpbp', sep='|')

conn.commit()
