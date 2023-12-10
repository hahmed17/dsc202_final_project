import psycopg2
import os

# MUST CREATE CONNECTION to Postgres before any calls are made
pg_conn = psycopg2.connect(
        dbname="public",
        user="postgres",
        password="root123",  # change this to your own password
        host="localhost",
        port="5432"
    )
pg_cursor = pg_conn.cursor()

# export dataframes to csv file and import to Postgres 
# income per community
income_table = """
    CREATE TABLE IF NOT EXISTS communitystats(
    CommunityAreaNumber VARCHAR PRIMARY KEY,
    CommunityAreaName VARCHAR(255),
    PercentHousingCrowded DECIMAL(4, 1),
    PercentHouseholdsBelowPoverty DECIMAL(4, 1),
    PercentAged16PlusUnemployed DECIMAL(4, 1),
    PercentAged25PlusNoHighSchoolDiploma DECIMAL(4, 1),
    PercentAgedUnder18OrOver64 DECIMAL(4, 1),
    PerCapitaIncome VARCHAR,
    HardshipIndex VARCHAR
    );
    """

# redlining scores
redlining_table = """
    CREATE TABLE IF NOT EXISTS redlinescores (
    holc_id VARCHAR(5),
    holc_grade text,
    community text,
    NID INT,
    POP2010 INT,
    POP2000 INT,
    POPCH INT,
    POPPERCH NUMERIC(10, 6),
    popplus INT,
    popneg INT
);
    """

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
    with open(os.path.join('../data/', v), 'r', encoding='utf-8-sig') as file:
        next(file)
        pg_cursor.copy_from(file, k, sep=',')
    
# Close the PostgreSQL cursor and connection - MUST BE RUN AFTER ANY CALLS TO POSTGRES
if pg_cursor is not None:
    pg_cursor.close()
if pg_conn is not None:
    pg_conn.close()