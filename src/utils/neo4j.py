from neo4j import GraphDatabase
import pandas as pd

def get_neo4j_session():
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "root1234"))
    session =  driver.session()
    return session

def print_neo4j_query(session, query):
    result = session.run(query)
    for r in result:
        print(r)

def get_neo4j_result_as_df(session, query):
    result = session.run(query)
    records = [dict(record) for record in result]
    return pd.DataFrame(records)