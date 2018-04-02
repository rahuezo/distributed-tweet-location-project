from utils.configuration import MOVERS_TWEETS_DB_PATH, TWEETS_TBNAME, TWEETS_COLUMNS, WEATHER_INFO_DB_PATH, WEATHER_INFO_COLUMNS
from utils.separation import chunkify
from utils.database import Database
from utils.validation import is_weather
from utils.timing import fips_date_mapper

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
    return map(lambda x: (x[0], x[1], x[2], x[3], 
        fips_date_mapper(user_chronology_db, x[0], x[3]), is_weather(x[1])), tweet_batch)


if __name__ == '__main__':
    try: 
        mover_tweets_db_files = fd.askopenfilenames(title='Choose databases with mover tweets')
        user_chronology_db_file = fd.askopenfilename(title='Choose database with user chronology')

        if not mover_tweets_db_files or not user_chronology_db_file: 
            raise Exception('\nNo databases selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()
    
    user_chronology_db = Database(user_chronology_db_file)

    for i, mover_tweets_db_file in enumerate(mover_tweets_db_files):
        # if int(ceil((float(i) / len(mover_tweets_db_files))*100)) % 25 == 0 or i == len(mover_tweets_db_files) - 1: 
        print "\tProcessing db file {} out of {}".format(i + 1, len(mover_tweets_db_files))

        mover_tweets_db = Database(mover_tweets_db_file)        
        
        weather_info_db = Database(os.path.join(WEATHER_INFO_DB_PATH, mover_tweets_db_file.split('/')[-1]))
        weather_info_tb = weather_info_db.create_table('tweets', WEATHER_INFO_COLUMNS)

        ntweets = mover_tweets_db.select('SELECT COUNT(user_id) FROM tweets').fetchone()[0]

        mover_tweets_db.select('SELECT * FROM tweets')

        weather_info_db.cursor.execute('BEGIN')

        processed_tweets = 0 

        while True: 
            if int(ceil((float(processed_tweets) / ntweets)*100)) % 25 == 0 or processed_tweets == ntweets - 1: 
                print "\t\t {} out of {}".format(processed_tweets, ntweets)

            tweets = mover_tweets_db.cursor.fetchmany(TWEET_BATCH_SIZE)
            processed_tweets += len(tweets)

            if tweets:   
                weather_info_db.insert('INSERT INTO tweets VALUES(?, ?, ?, ?, ?, ?)', process_tweet_batch(tweets, user_chronology_db), many=True)  
            else: 
                break 
        print 
        weather_info_db.connection.commit()
    user_chronology_db.connection.close()

    print '\nElapsed Time: {}s\n'.format(round(time.time() - s, 2))
    print 'Size: {}\tTweet Batch Size: {}\n'.format(TEST_SIZE if TEST_SIZE else 'All', TWEET_BATCH_SIZE)
