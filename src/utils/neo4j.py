from neo4j import GraphDatabase

def get_neo4j_session():
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "root1234"))
    session =  driver.session()
    return session