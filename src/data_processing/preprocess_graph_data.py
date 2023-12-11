import geopandas as gpd
import pandas as pd
import re
from geopy import distance
import numpy as np
from shapely.geometry import Point
import os

def process_route(routes):
    from_stations = []
    to_stations = []
    first_coords_x = []
    first_coords_y = []
    first_coords = []
    last_coords_x = []
    last_coords_y = []
    last_coords = []

    for index, row in routes.explode().iterrows():
        stations = row['DESCRIPTIO'].split(' to ')
        if len(stations) < 2:
            stations = row['DESCRIPTIO'].split(' ot ')
            if len(stations) < 2:
                continue

        stations = [re.sub(r'\([^)]*\)', '', s).strip() for s in stations]
            
        # print(stations)
        route_geom = row['geometry']
        first = route_geom.coords[0]
        last = route_geom.coords[-1]
        
        first_coords_x.append(first[0])
        first_coords_y.append(first[1])
        first_coords.append(Point(first))

        last_coords_x.append(last[0])
        last_coords_y.append(last[1])
        last_coords.append(Point(last))

        from_stations.append(stations[0])
        to_stations.append(stations[1])

        # add bidirectional edges
        first_coords_x.append(last[0])
        first_coords_y.append(last[1])
        first_coords.append(Point(last))

        last_coords_x.append(first[0])
        last_coords_y.append(first[1])
        last_coords.append(Point(first))

        from_stations.append(stations[1])
        to_stations.append(stations[0])

    df = pd.DataFrame(
        {
            'src_station': from_stations,
            'dest_stations': to_stations
        }
    )

    stations = pd.DataFrame(
        {
            'station': from_stations,
            'loc': first_coords
        }
    )
    stations = stations.drop_duplicates()
    stations = gpd.GeoDataFrame(stations).set_geometry('loc')
    return df, stations

# process metra
metra_routes = gpd.read_file("data/raw/MetraLines").to_crs(epsg=4326)

metra_routes, metra_stations = process_route(metra_routes)
metra_routes.to_csv("data/processed/MetraRoutes.csv", index=False)

if not os.path.exists('data/processed/MetraStations'):
    os.makedirs('data/processed/MetraStations')

metra_stations.to_csv('data/processed/MetraStations.csv', index=False)
metra_stations.to_file('data/processed/MetraStations/MetraStations.shp', index=False)

# train
train_routes = gpd.read_file("data/raw/CTA_RailLines").to_crs(epsg=4326)
train_routes, train_stations = process_route(train_routes)

train_routes.to_csv("data/processed/TrainRoutes.csv", index=False)

if not os.path.exists('data/processed/TrainStations'):
    os.makedirs('data/processed/TrainStations')

train_stations.to_file('data/processed/TrainStations/TrainStations.shp', index=False)
train_stations.to_csv('data/processed/TrainStations.csv', index=False)

