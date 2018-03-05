import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__)).replace('utils', '')

DATE_FROM_FILE_SIZE = 10 #'2014_03_23_02_usgeo'
N_CHUNKS = 10 
PATH_SEP = '\\'

TWEETS_TBNAME = 'tweets'
TWEETS_COLUMNS = 'user_id INT, tweet_text TEXT, tweet_location TEXT, tweet_date TEXT, fips INT'

RESULTS_DIR = 'results'
RESULTS_DIR_PATH = os.path.join(ROOT_DIR, RESULTS_DIR)

if not os.path.exists(RESULTS_DIR_PATH): 
    os.makedirs(RESULTS_DIR_PATH)
    
