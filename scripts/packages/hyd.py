import psycopg2
import pprint
import plotly
import pandas as pd

conn = psycopg2.connect(database="24850_hs2", user="andydixon", host="localhost", port="5666")

cursor = conn.cursor()
cursor.execute('SELECT t.* FROM (\
                    SELECT sr_survey_id, survey_distance, survey_speed, \
                        row_number() OVER(ORDER BY sr_survey_id asc, survey_distance ASC) AS row\
                    FROM scrim.tbl_sr_s01 where sr_survey_id = 413) t\
                WHERE t.row % 10 = 0')
rows = cursor.fetchall()
conn.close()
