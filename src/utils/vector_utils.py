import geopandas as gpd
import pandas as pd

def count_and_merge(point_df, polygon_df, rename_col):
    # Perform a spatial join to find points within each polygon
    result_gdf = gpd.sjoin(point_df, polygon_df, how='inner')

    # Count the number of points in each polygon
    point_count_per_polygon = result_gdf.groupby('index_right').size().reset_index(name='count')
    count_df = pd.merge(polygon_df.reset_index(), 
                            point_count_per_polygon.rename(columns={'index_right': 'index'}), 
                            how='left', 
                            on='index')
    count_df = count_df.fillna(0).drop(columns='index').rename(columns={'count': rename_col})
    
    return count_df