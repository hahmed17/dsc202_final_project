import psycopg2
import os
from src.utils.postgres import *
from src.utils.file_utils import *

pg_conn, pg_cursor = get_postgres_cursor()

# export dataframes to csv file and import to Postgres 

schema_files = os.listdir('data/schemas')

all_tables = []
for f in schema_files:
    all_tables.append(get_file_contents(os.path.join('data/schemas', f)))

for table in all_tables:
    pg_cursor.execute(table)

# table-filename associations 
csv_tables = {
    'communitystats': 'Per_Capita_Income.csv',
    'redlinescores': 'redlining_per_neighborhood.csv',
    'redlinedcrimedata': 'Crimes.csv',
    'schoolsdata': 'Schools.csv',
    'service_requests': '311_Service_Requests_vacancies.csv',
    'crimedata': 'Crimes_full.csv',
    'schoolscores': 'SchoolScores.csv'
}

for k in csv_tables.keys():
    v = csv_tables[k]
    with open(os.path.join('data/', v), 'r', encoding='utf-8-sig') as file:
        next(file)
        query='''
        COPY {} FROM STDIN WITH CSV HEADER DELIMITER ','
        '''.format(k)
        pg_cursor.copy_expert(query, file)
 
pg_conn.commit()
stop_postgres(pg_conn, pg_cursor)