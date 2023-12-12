
from pymongo import MongoClient
import json
import glob
import os
import pandas as pd
from src.utils.mongodb import get_mongodb_db

path = os.getcwd()
client = MongoClient()
json_files = []  # store json file names in list

# function to load json files
def load_json(filepath):
    with open(filepath) as f:
        result = json.load(f)
    return result[0]

json_filepath = path = r'data/jsons/*.json'
json_filepaths_list = glob.glob(json_filepath)
neigborhood_jsons = []


# get files for desired neighborhoods
df = pd.read_csv('data/Per_Capita_Income.csv')
community_list = df['COMMUNITY AREA NAME'].tolist()[:-1]

db = get_mongodb_db()

for f in json_filepaths_list:
    f_stem = os.path.splitext(os.path.basename(f))[0]
    if f_stem in community_list: 
        json_content = load_json(f)
        neigborhood_jsons.append(json_content)  # load each json file
        # Add filename as a field
        json_content["filename"] = f_stem
        # Insert into MongoDB
        db.neighborhoods.insert_one(json_content)