import geopandas as gpd
import geodatasets
import pandas as pd

redlining_df = gpd.read_file("data/raw/ILChicago1940").to_crs('EPSG:3857')
chicago_neighborhoods_df = gpd.read_file(geodatasets.get_path("geoda.chicago_commpop")).to_crs('EPSG:3857')

neighborhood_area = chicago_neighborhoods_df['geometry'].area.values
chicago_neighborhoods_df['neighborhood_area'] = neighborhood_area

# save geometric columns as geoseries
redline_coordinates = gpd.GeoSeries(redlining_df['geometry'])
neighborhood_coordinates = gpd.GeoSeries(chicago_neighborhoods_df['geometry'])

# Find overlaps between redlined districts and current day communities
overlapping_gdf = redlining_df.sjoin(chicago_neighborhoods_df, how='inner', predicate='intersects')
region_area = overlapping_gdf['geometry'].area.values

overlapping_gdf['region_area'] = region_area

overlapping_gdf = overlapping_gdf.drop(columns=['name', 'geometry', 'index_right']).reset_index(drop=True)

overlapping_gdf.to_csv('data/redlining_per_neighborhood.csv', sep=',', index=False)