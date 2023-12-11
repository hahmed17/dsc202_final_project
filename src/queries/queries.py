import geopandas as gpd
import geodatasets
import pandas as pd
import psycopg2
import os
from neo4j import GraphDatabase
from geopy import distance
import numpy as np
from src.utils.postgres import *
from src.utils.neo4j import *

def run_income_query(pg_cursor):
    income_query = '''
        WITH t AS
        (
        SELECT holc_grade,
        SUM(PerCapitaIncome * region_area) AS weighted_income,
        SUM(PercentHouseholdsBelowPoverty * region_area) AS weighted_poverty,
        SUM(region_area) as sum_area
        FROM redlinescores r, communitystats c
        WHERE r.NID = c.CommunityAreaNumber
        GROUP BY holc_grade
        )
        SELECT holc_grade as "Redline Grade", 
        weighted_income/sum_area as "Average Per Capita Income", 
        weighted_poverty/sum_area as "Average Poverty Rate" 
        FROM t
        ORDER BY holc_grade;
        '''
    
    execute_postgres(pg_cursor, income_query)
    print_pretty_table(pg_cursor)

def run_transport_query(neo4j_session, pg_cursor):
    

if __name__ == '__main__':
    pg_conn, pg_cursor = get_postgres_cursor()
    neo4j_session = get_neo4j_session()
    
    print("****************** Query 1 ********************")
    print("a. Running the query about poverty and income")
    print("***********************************************")
    run_income_query(pg_cursor)
    
    print("***********************************************")
    print("b. Running the query about public transport")
    print("***********************************************")
    run_transport_query(neo4j_session, pg_cursor)

    
    stop_postgres(pg_conn, pg_cursor)