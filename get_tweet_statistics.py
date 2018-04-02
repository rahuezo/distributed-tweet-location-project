from utils.configuration import MOVERS_TWEETS_DB_PATH, TWEETS_TBNAME, TWEETS_COLUMNS, WEATHER_INFO_DB_PATH, WEATHER_INFO_COLUMNS
from utils.separation import chunkify
from utils.database import Database
from utils.validation import is_weather

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
        mover_tweets_db_files = fd.askopenfilenames(title='Choose databases with mover tweets')

        if not mover_tweets_db_files: 
            raise Exception('\nNo database selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()
        
    print '\nElapsed Time: {}s\n'.format(round(time.time() - s, 2))
    print 'Size: {}\tTweet Batch Size: {}\n'.format(TEST_SIZE if TEST_SIZE else 'All', TWEET_BATCH_SIZE)
