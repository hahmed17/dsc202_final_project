import geopandas as gpd
import geodatasets
import pandas as pd

redlining_df = gpd.read_file("data/raw/ILChicago1940")
chicago_neighborhoods_df = gpd.read_file(geodatasets.get_path("geoda.chicago_commpop"))

# save geometric columns as geoseries
redline_coordinates = gpd.GeoSeries(redlining_df['geometry'])
neighborhood_coordinates = gpd.GeoSeries(chicago_neighborhoods_df['geometry'])

# Find overlaps between redlined districts and current day communities
overlapping_gdf = redlining_df.sjoin(chicago_neighborhoods_df, how='inner')
overlapping_gdf = overlapping_gdf.drop(columns=['name', 'geometry', 'index_right']).reset_index(drop=True)

overlapping_gdf.to_csv('data/redlining_per_neighborhood.csv', sep=',', index=False)