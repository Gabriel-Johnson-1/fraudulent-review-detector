# %%
from pymongo import MongoClient
import pandas as pd
import numpy as np
from tqdm import trange
from pprint import pprint

# %%
def move_documents(source_col, dest_col, filter={}):
    docs_to_move = list(source_col.find(filter))

    if not docs_to_move:
        print("No documents moved")
        return
    
    dest_col.insert_many(docs_to_move)

# %%
uri = "mongodb://localhost:27017"
client = MongoClient(host=uri)

# %%
mydb = client['reviews']

# %%
if 'google_reviews' in mydb.list_collection_names():
    mydb.drop_collection('google_reviews')

# %%
my_cols = mydb.list_collection_names()
dest_col = mydb['google_reviews']

pbar = trange(len(my_cols), desc='Inserting into "google_reviews" collection')
for i in trange(len(my_cols)):
    source_col = mydb[my_cols[i]]
    move_documents(source_col=source_col, dest_col=dest_col, filter={})
    pbar.set_description(f"Moving documents from {source_col.name}.")

pbar.close()
# %%
client.close()
print("Successfully moved all documents to 'google_reviews'")
