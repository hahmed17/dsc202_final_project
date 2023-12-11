import psycopg2
import os
from prettytable import PrettyTable
from tabulate import tabulate

def get_postgres_cursor():
    # MUST CREATE CONNECTION to Postgres before any calls are made
    pg_conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="root123",  # change this to your own password
            host="localhost",
            port="5432"
        )
    pg_cursor = pg_conn.cursor()
    return pg_conn, pg_cursor

def stop_postgres(pg_conn, pg_cursor):
    if pg_cursor is not None:
        pg_cursor.close()
    if pg_conn is not None:
        pg_conn.close()

def execute_postgres(pg_cursor, query):
    pg_cursor.execute(query)

def print_pretty_table(pg_cursor):
    rows = pg_cursor.fetchall()
   
    columns = [desc[0] for desc in pg_cursor.description]
    # Create a PrettyTable object and add columns
    table = PrettyTable(columns)

    # Add rows to the table
    for row in rows:
        table.add_row(row)

    print(table)


def print_pretty_df(df):
   # Create a PrettyTable object
    table = PrettyTable()

    # Add column names
    table.field_names = df.columns
    # table.float_format = ".4f"

    # Add each row of data
    for row in df.iterrows():
        table.add_row(row[1].tolist())

    # Print the table
    print(table)
