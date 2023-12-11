import psycopg2
import os
from src.utils.postgres import *
from src.utils.file_utils import *

pg_conn, pg_cursor = get_postgres_cursor()

# export dataframes to csv file and import to Postgres 
# income per community
income_table = get_file_contents('data/schemas/income.sql') 

# redlining scores
redlining_table = get_file_contents('data/schemas/redlining.sql')
all_tables = [income_table, redlining_table]

for table in all_tables:
    pg_cursor.execute(table)

# table-filename associations 
csv_tables = {
    'communitystats': 'Per_Capita_Income.csv',
    'redlinescores': 'redlining_per_neighborhood.csv'
}

for k in csv_tables.keys():
    v = csv_tables[k]
    with open(os.path.join('data/', v), 'r', encoding='utf-8-sig') as file:
        next(file)
        pg_cursor.copy_from(file, k, sep=',')
    
pg_conn.commit()
stop_postgres(pg_conn, pg_cursor)