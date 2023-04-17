import pandas as pd
import os
from os import makedirs
import shutil
from logger import logging
from exception import CustomException
import sys

RAW_DIR = os.path.join('data', 'raw')
COMBINED_DIR = os.path.join('data', 'combined')
PROCESSED_DIR = os.path.join('data', 'processed')
makedirs(PROCESSED_DIR, exist_ok=True)
makedirs(COMBINED_DIR, exist_ok=True)


def processing_data():
    """
    This function concat each individual file to Single file for further processing.
    Move the files from raw directory to processed directory.
    :return: None
    """
    try:
        dfs = []
        for filename in os.listdir(RAW_DIR):
            if filename.endswith(".csv"):
                df = pd.read_csv(os.path.join(RAW_DIR, filename))
                year = os.path.splitext(filename)[0]
                df.insert(1, 'year', year)
                dfs.append(df)
                src_file = os.path.join(RAW_DIR, filename)
                dst_file = os.path.join(PROCESSED_DIR, filename)
                # Move the file from raw  folder to processed directory
                shutil.move(src_file, dst_file)
                logging.info("file has been moved from source folder %s to destination folder %s", src_file, dst_file)
        combined_df = pd.concat(dfs).reset_index(drop=True)
        final_df = combined_df.iloc[:, 1:]
        combined_file = os.path.join(COMBINED_DIR, 'combined.csv')
        final_df.to_csv(combined_file, mode='a', header=not os.path.exists(combined_file))
        logging.info("All the file has been successfully processed")
    except Exception as e:
        raise CustomException(e, sys)
