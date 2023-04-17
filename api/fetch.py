import pandas as pd
import requests
import time
import os
from os import makedirs
from os.path import join, exists
from datetime import datetime, date, timedelta
import config
from logger import logging
from exception import CustomException
import sys

API_ENDPOINT = config.API_ENDPOINT
Data_DIR = os.path.join('data', 'articles')
RAW_DIR = os.path.join('data', 'raw')
PROCESSED_DIR = os.path.join('data', 'Processed')
makedirs(Data_DIR, exist_ok=True)
makedirs(RAW_DIR, exist_ok=True)

my_params = {
    'q': config.QUERY,
    'from-date': config.FROMDATE,
    'to-date': config.TODATE,
    'order-by': config.ORDERBY,
    'page-size': config.PAGESIZE,
    'page': config.PAGE,
    'api-key': config.APIKEY
}


def data_extraction():

    '''
    This function extracts information from API  and stored each csv file  in raw directory.
    '''

    try:
        # Read the  date from the text file mentioned the last file extracted from API
        with open("startdate.txt") as f:
            date_obj = f.read().strip()
        # Start date for data extraction
        start_date = (datetime.strptime(date_obj, '%Y-%m-%d')).date()
        logging.info("start_date %s", start_date)
        # Current date
        end_date = date.today()
        logging.info("end_date %s", end_date)
        daysrange = range((end_date - start_date).days+1)
        logging.info("daysrange %s", daysrange)
        for daycount in daysrange:
            dt = start_date + timedelta(days=daycount)
            datestr = dt.strftime('%Y-%m-%d')

            fname = join(RAW_DIR, datestr + '.csv')
            if not exists(fname):
                # Download extracted  file from API
                logging.info("Downloading %s", datestr)
                my_params['from-date'] = datestr
                my_params['to-date'] = datestr
                all_results = []
                current_page = 1
                total_pages = 1
                while current_page <= total_pages:
                    logging.info("current_pages %s", current_page)
                    response = requests.get(API_ENDPOINT, my_params)
                    if response.status_code == 200:
                        logging.info("API request on date %s was successful", my_params['from-date'])
                        data = response.json()
                        all_results.extend(data['response']['results'])
                        current_page += 1
                        total_pages = data['response']['pages']
                        with open('startdate.txt', 'w') as f:
                            f.write(my_params['from-date'])
                        logging.info("total_pages %s", total_pages)
                    else:
                        logging.error("API request on date %s was unsuccessful. Status code: %s", my_params['from-date'],
                                      response.status_code)
                    time.sleep(20)
                df = pd.DataFrame(all_results)
                df.to_csv(os.path.join(fname))
    except Exception as e:
        raise CustomException(e, sys)

