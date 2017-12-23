import psycopg2
import pandas as pd
import datetime

date = datetime.datetime.now() - datetime.timedelta(days=1)
print(type(date))
sql_date = date.strftime('%Y-%m-%d')
print(type(sql_date))
conn = psycopg2.connect("host=localhost dbname=nhl user=matt")
cur = conn.cursor()
top5_query = "SELECT event_player_1, event_team, sum(xg) AS ixG FROM \
        masternhlpbp WHERE game_date = '{}' GROUP BY event_player_1, event_team\
        ORDER BY \
        SUM(xg) DESC LIMIT 5;".format(sql_date)
top5 = pd.read_sql(top5_query, conn)

print(top5.head())
