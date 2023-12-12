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

epsg = 26916

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
    result_df = count_and_merge(train_df, result_df, 'train_count').to_crs(f'EPSG:{epsg}')

    # get centroids
    centroids = gpd.GeoDataFrame(geometry=result_df.centroid).to_crs(f'EPSG:{epsg}')
    centroids['holc_id'] = result_df['holc_id']
    
    # get the closest metra and train stations
    metra_stations = centroids.sjoin_nearest(metra_df.to_crs(f'EPSG:{epsg}'), distance_col='metra_dist').rename(columns={'station': 'metra_station'})
    train_stations = centroids.sjoin_nearest(train_df.to_crs(f'EPSG:{epsg}'), distance_col='train_dist').rename(columns={'station': 'train_station'})

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
            SELECT c.holc_grade, c.holc_id, Date, PrimaryType, Description, CommunityArea, region_area,
            CASE WHEN PrimaryType ILIKE ANY(ARRAY['Criminal Sexual Assault', 'Homicide', 'Robbery', 'Motor Vehicle Theft'])
              THEN 1 ELSE 0 END AS violent
            FROM redlinedcrimedata c, redlinescores r
            WHERE c.holc_id = r.holc_id 
        ),
        s AS
        (
            SELECT t.holc_grade, COUNT(*) as total_crime, SUM(violent) as violent_crime, SUM(region_area) as total_area
            FROM t
            GROUP BY t.holc_grade
        ),
        r AS
        (
            SELECT sum(total_area) as full_area
            FROM s
        )
        SELECT holc_grade as "Redlining Grade", 
        total_crime as "Total Crime incidents", 
        violent_crime as "Violent Crime Incidents", 
        total_crime/total_area*1000000 as "Crime per unit area", 
        violent_crime/total_area*1000000 as "Violent Crime per unit area",
        total_area/r.full_area*100 as "Percent of Area"
        FROM s, r;
        '''
    
    #PrimaryType ILIKE ANY(ARRAY['Criminal Sexual Assault', 'Assault/Battery', 'Homicide', 'Robbery', 'Motor Vehicle Theft'
    execute_postgres(pg_cursor, crime_query)
    print_pretty_table(pg_cursor)

def run_grocery_query(neo4j_session, pg_cursor):
    metra_dist_away = 1000
    train_dist_away = 1000
    
    cutoff_distance_metra = 3
    cutoff_distance_train = 2

    # open the redlining database
    redlining_df = gpd.read_file('data/raw/ILChicago1940')

    # open the metra and train stations data
    metra_df = gpd.read_file('data/processed/MetraStations/').set_crs('EPSG:4326')
    train_df = gpd.read_file('data/processed/TrainStations/').set_crs('EPSG:4326')

    # open the grocery stores data
    grocery_df = pd.read_csv('data/Grocery_Stores.csv', encoding='utf-8-sig').dropna()

    grocery_df = gpd.GeoDataFrame(grocery_df, geometry=[Point(float(x.split()[1][1:]), float(x.split()[2][:-1])) for x in grocery_df['Location']])
    grocery_df = grocery_df.set_crs('EPSG:4326')

    grocery_redlined_df = redlining_df.sjoin(grocery_df, how='right', predicate='intersects').drop(columns=['index_left', 'name'])
    
    # number of grocery stores
    result_df = count_and_merge(grocery_redlined_df, redlining_df, 'grocery_count')
    
    # get centroids
    centroids = gpd.GeoDataFrame(geometry=result_df.centroid).to_crs(f'EPSG:{epsg}')
    centroids['holc_grade'] = result_df['holc_grade']
    centroids['holc_id'] = result_df['holc_id']

    # get the closest metra and train stations to the centroids
    metra_stations = centroids.sjoin_nearest(metra_df.to_crs(f'EPSG:{epsg}'), distance_col='metra_dist').reset_index()
    train_stations = centroids.sjoin_nearest(train_df.to_crs(f'EPSG:{epsg}'), distance_col='train_dist').reset_index()

    metra_stations = metra_stations[metra_stations['metra_dist'] <= metra_dist_away]
    train_stations = train_stations[train_stations['train_dist'] <= train_dist_away]
    
    # get the closest metra and train stations to the grocery stores
    gmetra_stations = grocery_df.to_crs(f'EPSG:{epsg}').sjoin_nearest(metra_df.to_crs(f'EPSG:{epsg}'), distance_col='metra_dist').reset_index()
    gtrain_stations = grocery_df.to_crs(f'EPSG:{epsg}').sjoin_nearest(train_df.to_crs(f'EPSG:{epsg}'), distance_col='train_dist').reset_index()
    gmetra_stations = gmetra_stations[gmetra_stations['metra_dist'] <= metra_dist_away]
    gtrain_stations = gtrain_stations[gtrain_stations['train_dist'] <= train_dist_away]
    
    # get all pairs shortest distance for train stations
    train_query = '''
    CALL gds.allShortestPaths.stream('train_graph')
    YIELD sourceNodeId, targetNodeId, distance
    RETURN gds.util.asNode(sourceNodeId).name as src_station,
    gds.util.asNode(targetNodeId).name as target_station,
    distance as distance
    '''
    train_distances = get_neo4j_result_as_df(neo4j_session, train_query)

    # get all pairs shortest distance for metra stations
    metra_query = '''
    CALL gds.allShortestPaths.stream('metra_graph')
    YIELD sourceNodeId, targetNodeId, distance
    RETURN gds.util.asNode(sourceNodeId).name as src_station,
    gds.util.asNode(targetNodeId).name as target_station,
    distance as distance
    '''
    metra_distances = get_neo4j_result_as_df(neo4j_session, metra_query)
    
    merged_df = pd.merge(
        metra_stations,
        metra_distances,
        left_on='station',
        right_on='src_station',
        how='inner'
    )

    merged_df = merged_df[merged_df['distance'] <= cutoff_distance_metra]
    merged_df = pd.merge(
        merged_df,
        gmetra_stations,
        left_on='target_station',
        right_on='station',
        how='inner'
    )
    merged_df = merged_df.groupby('holc_id').size().reset_index(name='metra_exist')
    merged_df['metra_exist'] = (merged_df['metra_exist'] >= 0).astype(int)
    
    #####################################################
    
    # count the number of redlined districts
    count_df = redlining_df.groupby('holc_grade').size().reset_index(name='count')
    
    result_df = pd.merge(result_df, merged_df, on='holc_id', how='left').fillna(0)
    result_df['metra_exist'] = np.logical_or(result_df['metra_exist'], result_df['grocery_count']>0)
    
    #########################################################
    merged_df = pd.merge(
        train_stations,
        train_distances,
        left_on='station',
        right_on='src_station',
        how='inner'
    )

    merged_df = merged_df[merged_df['distance'] <= cutoff_distance_train]
    merged_df = pd.merge(
        merged_df,
        gtrain_stations,
        left_on='target_station',
        right_on='station',
        how='inner'
    )
    merged_df = merged_df.groupby('holc_id').size().reset_index(name='train_exist')
    merged_df['train_exist'] = (merged_df['train_exist'] >= 0).astype(int)
    
    result_df = pd.merge(result_df, merged_df, on='holc_id', how='left').fillna(0)
    result_df['train_exist'] = np.logical_or(result_df['train_exist'], result_df['grocery_count']>0)
    

    result_df = result_df.groupby('holc_grade').agg({'grocery_count': 'sum',
                                                     'metra_exist': 'sum',
                                                     'train_exist': 'sum'})
    result_df = pd.merge(result_df, count_df, how='inner', on='holc_grade')
    
    result_df['metra_exist_percent'] = result_df['metra_exist'].values / count_df['count'].values * 100
    result_df['train_exist_percent'] = result_df['train_exist'].values / count_df['count'].values * 100
    
    result_df = result_df.drop(columns=['metra_exist', 'train_exist', 'count'])

    result_df.columns = ['Redlining grade', 
                         '# Grocery Stores', 
                         '% of districts with a Metra Route to Grocery Store',
                         '% of districts with a Train Route to Grocery Store']
    result_df = result_df.reset_index(drop=True)

    print_pretty_df(result_df)

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
    
    print("***********************************************")
    print("d. Running the query about grocery stores")
    print("***********************************************")
    run_grocery_query(neo4j_session, pg_cursor)
    
    stop_postgres(pg_conn, pg_cursor)