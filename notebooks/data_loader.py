#%%
from nltk.corpus import stopwords
from pymongo import MongoClient
import matplotlib.pyplot as plt
from textblob import TextBlob
from pprint import pprint
import seaborn as sns 
import pandas as pd
import numpy as np
import pymongo
import string
import nltk
import tqdm
import re

#%%
def text_cleaning(text):
    '''
    Make text lowercase, remove text in square brackets,remove links,remove special characters
    and remove words containing numbers.
    '''
    if text != None:
        text = text.lower()
        text = re.sub(r'\[.*?\]', '', text)
        text = re.sub(r"\\W"," ",text) # remove special chars
        text = re.sub(r'https?://\S+|www\.\S+', '', text)
        text = re.sub(r'<.*?>+', '', text)
        text = re.sub(r'[%s]' % re.escape(string.punctuation), '', text)
        text = re.sub(r'\n', '', text)
        text = re.sub(r'\w*\d\w*', '', text)

    return text

#%%
def remove_stops(text):
    stop = stopwords.words('english')
    return ' '.join([word for word in text.split() if word not in (stop)])


def pos_tag(text):
    return TextBlob(text).tags

def combine_tags(text):
    return " ".join(["/".join(text) for text in text])


# %%
def connect_to_db(
        uri: str = 'mongodb://localhost:27017', 
        verbose: int | bool = 0) -> pymongo.synchronous.database.Database:
    """
    Docstring for connect_to_db
    
    :param uri: uri of DB to connect to ("mongodb://localhost:27017")
    :type uri: str
    :param verbose: display if connection is successful or not
    :type verbose: int | bool
    :return: a database object
    :rtype: pymongo.synchronous.database.Database
    """
    
    client = MongoClient(uri)
    if verbose:
        print("Connection Successful" if 'reviews' in client.list_database_names() else "Connection Error")
    return client['reviews']

# %%
def connect_to_col(
        db: pymongo.synchronous.database.Database, 
        col:str = 'google_reviews', 
        verbose: int | bool = 0) -> pymongo.synchronous.collection.Collection: 
    '''
    Docstring for connect_to_col
    
    :param db: Database
    :type db: pymongo.synchronous.database.Database
    :param col: collection to get from database ("google_reviews")
    :type col: str
    :param verbose: Say how many documents are in the collection
    :type verbose: int | bool
    :return: collection object
    :rtype: pymongo.synchronous.collection.Collection
    '''
    col=db[col]
    if verbose:
        print(f"{col.count_documents({})} Total Documents in 'google_reviews' collection")
    return col


#%%
def create_google_dataframe(
        col: pymongo.synchronous.collection.Collection,
        cols_to_use: str | list[str] = ['_id', 'author', 'city', 
                                        'description', 'last_modified_date', 
                                        'likes', 'owner_responses', 'rating', 
                                        'review_date', 'source'],
        clean_desc: int | bool = 1,
        clean_owner_response: int | bool = 1,
        text_only: int | bool = 1,
        remove_stop_words: int | bool = 1,
        pos_tagging: int | bool = 1,
        combine_pos_tag: int | bool = 1,
        ) -> pd.DataFrame:
    """
    Docstring for create_google_dataframe
    
    :param col: collection to use
    :type col: pymongo.synchronous.collection.Collection
    :param cols_to_use: columns from collection to use
    :type cols_to_use: str | list[str]
    :param clean_desc: clean the description column
    :type clean_desc: int | bool
    :param clean_owner_response: clean the owner_response column
    :type clean_owner_response: int | bool
    :param text_only: only return columns with textual reviews
    :type text_only: int | bool
    :param remove_stop_words: remove stopwords from reviews column
    :type remove_stop_words: int | bool
    :param pos_tagging: tag parts of speech onto the words
    :type pos_tagging: int | bool
    :param combine_pos_tag: combine the part of speech tagging into a single string
    :type combine_pos_tag: int | bool
    :return: a dataframe object of the collection
    :rtype: DataFrame
    """
    def clean_description(val):
        if 'en' in val.keys():
            return val['en']
        else:
            return None
        
    def clean_owner(val):
        if 'en' in val.keys():
            return val['en']['text']
        else:
            return None
    
    df = pd.DataFrame(list(col.find()))
    df = df[cols_to_use]
    
    
    if clean_desc:
        df['review'] = df['description'].apply(clean_description)
        df.drop(columns='description', inplace=True, errors='ignore')
        df['review'] = df['review'].apply(text_cleaning)
    
    if clean_owner_response:
        df['owner_responses'] = df['owner_responses'].apply(clean_owner)
    
    if text_only:
        df = df.loc[df['review'].notna()]
    
    if remove_stop_words:
        df['review_sans_stop'] = df['review'].apply(remove_stops)
    
    if pos_tagging:
        df['pos'] = df['review_sans_stop'].apply(pos_tag)
    
    if combine_pos_tag:
        df['pos_combo'] = df['pos'].map(combine_tags)
        
    return df

        

#%%
def create_from_csv(
        file: str, 
        clean_text: int | bool = 1, 
        remove_stopwords: int | bool = 1,
        tag_pos: int | bool = 1,
        ) -> pd.DataFrame :
    
    df = pd.read_csv(file)

    if clean_text:
        df['text'] = df['text'].apply(text_cleaning)

    if remove_stopwords:
        df['text_sans_stop'] = df['text'].apply(remove_stops)

    
# %%
