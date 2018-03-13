import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__)).replace('utils', '')

DATE_FROM_FILE_SIZE = 10 #'2014_03_23_02_usgeo'
N_CHUNKS = 10 
PATH_SEP = '\\'

TWEETS_TBNAME = 'tweets'
TWEETS_COLUMNS = 'user_id INT, tweet_text TEXT, tweet_location TEXT, tweet_date TEXT, fips INT'

RESULTS_DIR = 'results'
RESULTS_DIR_PATH = os.path.join(ROOT_DIR, RESULTS_DIR)


USER_FIPS_DB = 'aggregated_data.db'
USER_FIPS_TBNAME = 'users_fips'
USER_FIPS_COLUMNS = 'user_id INT, fips INT'

USER_FIPS_COLUMNS_UNIQUE = 'user_id INT, fips INT, tweet_date TEXT, UNIQUE(user_id, fips)'


POTENTIAL_MOVERS_TBNAME = 'potential_movers'
POTENTIAL_MOVERS_COLUMNS = 'user_id INT'

MOVERS_DB_NAME = os.path.join(RESULTS_DIR_PATH, 'actual_movers.db')
MOVERS_TBNAME = 'movers'
MOVERS_COLUMNS = 'user_id INT, fips1 INT, date1 TEXT, fips2 INT, date2 TEXT'


USERS_TBNAME = 'users'
USERS_COLUMNS = 'user_id INT'


if not os.path.exists(RESULTS_DIR_PATH): 
    os.makedirs(RESULTS_DIR_PATH)
