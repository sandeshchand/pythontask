import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text


root_dir = os.path.dirname(os.path.abspath(__file__)).rsplit(os.sep, 1)[0]
file_path = 'api\\data\\combined\\combined.csv'
engine = create_engine('sqlite:///GuardianDB.sqlite', echo=False)


def insert_data():
    """
    Insert the csv file  to database.
    """
    df = pd.read_csv(file_path)
    df.to_sql(
        'Justin',
        engine,
        if_exists='replace',
        index=False,
        chunksize=500,
        dtype={
            "Unnamed: 0": Integer,
            "year": Text,
            "id": Text,
            "type": Text,
            "SectionId": Text,
            "SectionName": Text,
            "webPublicationDate": Text,
            "webTitle": Text,
            "webUrl": Text,
            "apiUrl": Text,
            "isHosted": Text,
            "pillarId": Text,
            "pillarName": Text,

        }
    )


def get_data():
    """
    Extract the data from database.
    :return: dataframe
    """
    #
    df = pd.read_sql(
        "SELECT * FROM Justin",
        con=engine,
        parse_dates=[
            'Year'
        ]
    )
    return df
