import geopandas as gpd
import geodatasets
import pandas as pd
from shapely.geometry import Point

redlining_df = gpd.read_file("data/raw/ILChicago1940").to_crs('EPSG:26916')
chicago_neighborhoods_df = gpd.read_file(geodatasets.get_path("geoda.chicago_commpop")).to_crs('EPSG:26916')

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

# preprocess crimes
crime_df = pd.read_csv('data/raw/Crimes.csv', parse_dates=['Date'], encoding='utf-8-sig').dropna()

# Convert the 'Location' column to a Point geometry
geometry = crime_df['Location'].apply(lambda loc: Point(Point(eval(loc)).y, Point(eval(loc)).x) )

crime_df = gpd.GeoDataFrame(crime_df, geometry=geometry).set_crs('EPSG:4326')
crime_df = redlining_df.sjoin(crime_df.to_crs('EPSG:26916'), how='inner', predicate='intersects')
crime_df = crime_df[['ID', 'holc_id', 'holc_grade', 'Date', 'Primary Type', 'Description', 'Location Description', 'Community Area']].reset_index(drop=True)
crime_df.to_csv('data/Crimes.csv', index=False)

# preprocess schools
school_df = pd.read_csv('data/raw/Schools.csv', encoding='utf-8-sig').dropna()

school_df = gpd.GeoDataFrame(school_df, geometry=[Point(float(x.split()[1][1:]), float(x.split()[2][:-1])) for x in school_df['the_geom']])
school_df = school_df.set_crs('EPSG:4326')
school_df = redlining_df.sjoin(school_df.to_crs('EPSG:26916'), how='right', predicate='intersects')
school_df = school_df[['School_ID', 'Short_Name', 'holc_id', 'holc_grade', 'Grade_Cat']].reset_index(drop=True)
school_df.to_csv('data/Schools.csv', index=False)
