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
from src.utils.vector_utils import *

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
    # open the redlining database
    redlining_df = gpd.read_file('data/raw/ILChicago1940')

    # open the bus, metra and train stations data
    bus_df = gpd.read_file('data/raw/CTA_BusStops/')
    metra_df = gpd.read_file('data/processed/MetraStations/')
    train_df = gpd.read_file('data/processed/TrainStations/')

    # number of bus stops
    result_df = count_and_merge(bus_df, redlining_df, 'bus_count')

    # number of metra stations
    result_df = count_and_merge(metra_df, result_df, 'metra_count')

    # number of train stations
    result_df = count_and_merge(train_df, result_df, 'train_count')

    # get centroids
    centroids = result_df.centroid

    spatial_idx = result_df.sindex
    print(result_df)
    print(centroids)

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