from utils.configuration import MOVERS_TWEETS_DB_PATH, TWEETS_TBNAME, TWEETS_COLUMNS, WEATHER_INFO_DB_PATH, WEATHER_INFO_COLUMNS
from utils.separation import chunkify
from utils.database import Database
from utils.modulars import get_tweet_distribution

from utils.timing import str2date
from math import ceil

import tkFileDialog as fd
import multiprocessing as mp
import time
import sys, os

if len(sys.argv) > 1: 
    TEST_SIZE = int(sys.argv[1])
    if TEST_SIZE < 0: 
        TEST_SIZE = None
else: 
    TEST_SIZE = 100

if len(sys.argv) > 2: 
    TWEET_BATCH_SIZE = int(sys.argv[2])
else: 
    TWEET_BATCH_SIZE = 10000


def process_tweet_batch(tweet_batch, user_chronology_db): 
    return map(lambda x: x + (is_weather(x[1]), ), tweet_batch)


if __name__ == '__main__':
    try: 
        weather_info_db_files = fd.askopenfilenames(title='Choose databases with weather info tweets')

        if not weather_info_db_files: 
            raise Exception('\nNo database selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()

    for i, weather_info_db_file in enumerate(weather_info_db_files):
        print "Processing db file {} out of {}".format(i + 1, len(weather_info_db_files))

        weather_info_db = Database(weather_info_db_file)

        unique_users = [user[0] for user in weather_info_db.select('SELECT DISTINCT user_id FROM tweets')]

        for user_id in unique_users: 
            print get_tweet_distribution(weather_info_db, user_id)
            print 

        weather_info_db.connection.close()
        
    print '\nElapsed Time: {}s\n'.format(round(time.time() - s, 2))
    print 'Size: {}\tTweet Batch Size: {}\n'.format(TEST_SIZE if TEST_SIZE else 'All', TWEET_BATCH_SIZE)
