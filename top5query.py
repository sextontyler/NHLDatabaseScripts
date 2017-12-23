import psycopg2
import pandas as pd
import datetime

top5 = 'dailytop5.txt'
date = datetime.datetime.now() - datetime.timedelta(days = 1)
sql_date = date.strftime('%Y-%m-%d')
conn = psycopg2.connect("host=localhost dbname=nhl user=matt")
cur = conn.cursor()
top5_query = "SELECT event_player_1, event_team, sum(xg) AS ixG FROM\
 masternhlpbp WHERE game_date = '{}' GROUP BY event_player_1, event_team\
 ORDER BY SUM(xg) DESC LIMIT 5;".format(sql_date)
print(top5_query)
cur.execute(top5_query)
rows = cur.fetchall()
with open(top5, 'w+') as top5:
    top5.write('{} ixG Leaders:\n'.format(sql_date))
    for row in rows:
        print(str(row))
        top5.write('{}\n'.format(str(row)))
