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

USER_ID_DBS_PATH = os.path.join(RESULTS_DIR_PATH, 'users') 
USER_ID_DB_NAME = os.path.join(USER_ID_DBS_PATH, '{}_{}.db')
USER_ID_TBNAME = 'movers'
USER_ID_COLUMNS = 'user_id INT, fips1 INT, date1 TEXT'


USER_CHRONOLOGY_DB = os.path.join(RESULTS_DIR_PATH, 'user_chronology.db')
USER_CHRONOLOGY_TB = 'user_fips'
USER_CHRONOLOGY_COLUMNS = 'user_id INT, fips1 INT, date1 TEXT'

USERS_TBNAME = 'users'
USERS_COLUMNS = 'user_id INT'

MOVERS_TWEETS_DB_PATH = os.path.join(RESULTS_DIR_PATH, 'movers')
WEATHER_INFO_DB_PATH = os.path.join(RESULTS_DIR_PATH, 'weather_info')

WEATHER_INFO_COLUMNS = 'user_id INT, tweet_text TEXT, tweet_location TEXT, tweet_date TEXT, fips INT, weather INT'

NUMBER_OF_UNIQUE_USERS = 141966518

TWEET_STATS_DB = os.path.join(RESULTS_DIR_PATH, 'weather_info', 'tweet_statistics.db')
TWEET_STATS_TB = 'statistics'
TWEET_STATS_COLUMNS = 'user_id INT, date_text TEXT, total_tweets INT, weather_tweets INT, fips INT'

# user_id, date, ntotal_tweets, nweather_tweets, fips

if not os.path.exists(RESULTS_DIR_PATH): 
    os.makedirs(RESULTS_DIR_PATH)

if not os.path.exists(MOVERS_TWEETS_DB_PATH): 
    os.makedirs(MOVERS_TWEETS_DB_PATH)

if not os.path.exists(WEATHER_INFO_DB_PATH): 
    os.makedirs(WEATHER_INFO_DB_PATH)
