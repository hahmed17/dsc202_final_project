from neo4j import GraphDatabase
import shutil
import os
from src.utils.neo4j import *

# get the import directory
def get_import_dir(session):
    settings = session.run('SHOW SETTINGS')
    for r in settings:
        if r['name'] == 'server.directories.import':
            import_dir = r['value']
            break
    return import_dir

# create session
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "root1234"))
session =  driver.session()

# get the neo4j import directory
import_dir = get_import_dir(session)

# copy all CSVs from data dir to neo4j import_dir
data_dir = "data/processed/"
for file in os.listdir(data_dir):
    if file[-3:] == 'csv':
        src_path = os.path.join(data_dir, file)
        shutil.copy(src_path, import_dir)


# import the data into a neo4j database
add_metra_stations = """
LOAD CSV WITH HEADERS FROM 'file:///MetraStations.csv' AS row
WITH row.station AS station
MERGE (p:MetraStation{name: station});
"""

add_metra_routes = """
LOAD CSV WITH HEADERS FROM 'file:///MetraRoutes.csv' AS row
MATCH (src:MetraStation {name: row.src_station}), (dest:MetraStation {name: row.dest_stations})
MERGE (src)-[:CONNECTS_TO]->(dest);
"""

add_train_stations = """
LOAD CSV WITH HEADERS FROM 'file:///TrainStations.csv' AS row
WITH row.station AS station
MERGE (p:TrainStation{name: station});
"""

add_train_routes = """
LOAD CSV WITH HEADERS FROM 'file:///TrainRoutes.csv' AS row
MATCH (src:TrainStation {name: row.src_station}), (dest:TrainStation {name: row.dest_stations})
MERGE (src)-[:CONNECTS_TO]->(dest);
"""
setup_gds_train = '''
CALL gds.graph.project('train_graph', 'TrainStation', 'CONNECTS_TO')
'''

setup_gds_metra = '''
CALL gds.graph.project('metra_graph', 'MetraStation', 'CONNECTS_TO')
'''

print_neo4j_query(session, add_metra_stations)
print_neo4j_query(session, add_metra_routes)
print_neo4j_query(session, add_train_stations)
print_neo4j_query(session, add_train_routes)
print_neo4j_query(session, setup_gds_metra)
print_neo4j_query(session, setup_gds_train)

