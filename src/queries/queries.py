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
from shapely.geometry import Point

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
    bus_df = gpd.read_file('data/raw/CTA_BusStops/').set_crs('EPSG:4326')
    metra_df = gpd.read_file('data/processed/MetraStations/').set_crs('EPSG:4326')
    train_df = gpd.read_file('data/processed/TrainStations/').set_crs('EPSG:4326')

    # number of bus stops
    result_df = count_and_merge(bus_df, redlining_df, 'bus_count')

    # number of metra stations
    result_df = count_and_merge(metra_df, result_df, 'metra_count')

    # number of train stations
    result_df = count_and_merge(train_df, result_df, 'train_count').to_crs('EPSG:32633')

    # get centroids
    centroids = gpd.GeoDataFrame(geometry=result_df.centroid).to_crs('EPSG:32633')
    centroids['holc_id'] = result_df['holc_id']
    
    # get the closest metra and train stations
    metra_stations = centroids.sjoin_nearest(metra_df.to_crs('EPSG:32633'), distance_col='metra_dist').rename(columns={'station': 'metra_station'})
    train_stations = centroids.sjoin_nearest(train_df.to_crs('EPSG:32633'), distance_col='train_dist').rename(columns={'station': 'train_station'})

    # get the closeness centrality of all train stations
    train_query = '''
    CALL gds.closeness.stream('train_graph')
    YIELD nodeId, score
    RETURN gds.util.asNode(nodeId).name AS train_station, score AS train_score
    '''

    train_centrality = get_neo4j_result_as_df(neo4j_session, train_query)
    train_df = pd.merge(train_stations, train_centrality, on='train_station')

    # get the closeness centrality of all metra stations
    query = '''
    CALL gds.closeness.stream('metra_graph')
    YIELD nodeId, score
    RETURN gds.util.asNode(nodeId).name AS metra_station, score AS metra_score
    '''
    
    metra_centrality = get_neo4j_result_as_df(neo4j_session, query)
    metra_df = pd.merge(metra_stations, metra_centrality, on='metra_station')

    result_df = pd.merge(result_df, metra_df, on='holc_id')
    result_df = pd.merge(result_df, train_df, on='holc_id')

    # keep the relevant columns only
    result_df = result_df[['holc_grade', 'bus_count', 'metra_count', 'train_count', 'metra_score', 'train_score', 'metra_dist', 'train_dist']]
    result_df['metra_dist'] = convert_m_to_mi(result_df['metra_dist'])
    result_df['train_dist'] = convert_m_to_mi(result_df['train_dist'])

    result_df = result_df.groupby('holc_grade').agg({'bus_count': 'sum',
                                                     'metra_count': 'sum',
                                                     'train_count': 'sum',
                                                     'metra_score': 'mean',
                                                     'train_score': 'mean',
                                                     'metra_dist': 'mean',
                                                     'train_dist': 'mean'})
    result_df.columns = ['# Bus Stops', 
                         '# Metra Stations', '# Train Stations',
                         'Avg. centrality of nearest Metra Station', 
                         'Avg. centrality of nearest Train Station',
                         'Avg. dist to metra (mi)',
                         'Avg. dist to train (mi)']
    result_df = result_df.reset_index().rename(columns={'holc_grade': 'Redlining grade'})
    print_pretty_df(result_df.iloc[:, 0:4])
    print_pretty_df(result_df.iloc[:, [0,4,5,6,7]])

def run_crime_query(pg_cursor):
    crime_query = '''
        WITH t AS
        (
            SELECT c.holc_grade
            FROM redlinedcrimedata c, redlinescores r
            WHERE c.holc_id = r.holc_id
        )
        SELECT holc_grade FROM t;
        '''
    
    #PrimaryType ILIKE ANY(ARRAY['Criminal Sexual Assault', 'Assault/Battery', 'Homicide', 'Robbery', 'Motor Vehicle Theft'
    execute_postgres(pg_cursor, crime_query)
    print_pretty_table(pg_cursor)

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

    print("***********************************************")
    print("c. Running the query about crime data")
    print("***********************************************")
    run_crime_query(pg_cursor)
    
    stop_postgres(pg_conn, pg_cursor)