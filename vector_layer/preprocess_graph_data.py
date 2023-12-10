import geopandas as gpd
import pandas as pd
import re
from geopy import distance
import numpy as np


def process_route(routes):
    from_stations = []
    to_stations = []
    first_coords = []
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
        
        first_coords.append(first)
        last_coords.append(last)
        from_stations.append(stations[0])
        to_stations.append(stations[1])

        # add bidirectional edges
        first_coords.append(last)
        last_coords.append(first)
        from_stations.append(stations[1])
        to_stations.append(stations[0])

    df = pd.DataFrame(
        {
            'src_station': from_stations,
            'src_loc': first_coords,
            'dest_stations': to_stations,
            'dest_loc': last_coords 
        }
    )


    stations = df.drop_duplicates('src_station')[['src_station', 'src_loc']].rename(columns={'src_station': 'station', 'src_loc': 'loc'})

    return df, stations


# process metra
metra_routes = gpd.read_file("../data/raw/MetraLines").to_crs(epsg=4326)

metra_routes, metra_stations = process_route(metra_routes)
metra_routes.to_csv("../data/processed/MetraRoutes.csv", index=False)
metra_stations.to_csv('../data/processed/MetraStations.csv', index=False)

# train
train_routes = gpd.read_file("../data/raw/CTA_RailLines").to_crs(epsg=4326)
train_routes, train_stations = process_route(train_routes)
train_routes.to_csv("../data/processed/TrainRoutes.csv", index=False)
train_stations.to_csv('../data/processed/TrainStations.csv', index=False)

