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


def process_tweet_batch(tweet_batch): 
    return map(lambda x: x + (is_weather(x[1]), ), tweet_batch)


# def process_user_chunk(user_chunk, current_tweets_db, mover_tweets_db): 
#     for user_id in user_chunk:     
#         current_user_tweets = current_tweets_db.select('SELECT * FROM tweets WHERE user_id={}'.format(user_id))
#         mover_tweets_db.insert('INSERT INTO tweets VALUES(?, ?, ?, ?, ?)', current_user_tweets, many=True)


# def process_db_file(user_chunks, tweet_db_file): 
#     mover_tweets_db = Database(os.path.join(MOVERS_TWEETS_DB_PATH, tweet_db_file.split('/')[-1]))
#     mover_tweets_tb = mover_tweets_db.create_table(TWEETS_TBNAME, TWEETS_COLUMNS)
    
#     mover_tweets_db.cursor.execute('BEGIN')
    
#     current_tweets_db = Database(tweet_db_file)
#     s = time.time()

#     for i, user_chunk in enumerate(user_chunks):
#         if int(ceil((float(i) / len(user_chunks))*100)) % 25 == 0 or i == len(user_chunks) - 1:
#             print "\n\tProcessing chunk {} out of {}".format(i + 1, len(user_chunks)) 
#         process_user_chunk(user_chunk, current_tweets_db, mover_tweets_db)

#     current_tweets_db.connection.close()
#     mover_tweets_db.connection.commit()
#     mover_tweets_db.connection.close()

#     print '\n\tElapsed Time for db file: {}s\n'.format(round(time.time() - s, 2))


if __name__ == '__main__':
    try: 
        mover_tweets_db_files = fd.askopenfilenames(title='Choose databases with mover tweets')

        if not mover_tweets_db_files: 
            raise Exception('\nNo database selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()
    
    for i, mover_tweets_db_file in enumerate(mover_tweets_db_files):         
        mover_tweets_db = Database(mover_tweets_db_file)        
        
        weather_info_db = Database(os.path.join(WEATHER_INFO_DB_PATH, mover_tweets_db_file.split('/')[-1]))
        weather_info_tb = weather_info_db.create_table('tweets', WEATHER_INFO_COLUMNS)

        mover_tweets_db.select('SELECT * FROM tweets')

        weather_info_db.cursor.execute('BEGIN')

        while True: 
            tweets = mover_tweets_db.cursor.fetchmany(TWEET_BATCH_SIZE)
            if tweets:   
                weather_info_db.insert('INSERT INTO tweets VALUES(?, ?, ?, ?, ?, ?)', process_tweet_batch(tweets), many=True)  
            else: 
                break 

        weather_info_db.connection.commit()
        
    print '\nElapsed Time: {}s\n'.format(round(time.time() - s, 2))
    print 'Size: {}\tTweet Batch Size: {}\n'.format(TEST_SIZE if TEST_SIZE else 'All', TWEET_BATCH_SIZE)
