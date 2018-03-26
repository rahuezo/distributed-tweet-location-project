from utils.configuration import MOVERS_TWEETS_DB_PATH, TWEETS_TBNAME, TWEETS_COLUMNS
from utils.separation import chunkify
from utils.database import Database

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
    USER_CHUNK_SIZE = int(sys.argv[2])
else: 
    USER_CHUNK_SIZE = 100000
    
def process_user_chunk(user_chunk, current_tweets_db, mover_tweets_db): 
    for user_id in user_chunk:     
        current_user_tweets = current_tweets_db.select('SELECT * FROM tweets WHERE user_id={}'.format(user_id))
        mover_tweets_db.insert('INSERT INTO tweets VALUES(?, ?, ?, ?, ?)', current_user_tweets, many=True)


def process_db_file(user_chunks, tweet_db_file): 
    mover_tweets_db = Database(os.path.join(MOVERS_TWEETS_DB_PATH, tweet_db_file.split('/')[-1]))
    mover_tweets_tb = mover_tweets_db.create_table(TWEETS_TBNAME, TWEETS_COLUMNS)
    
    mover_tweets_db.cursor.execute('BEGIN')
    
    current_tweets_db = Database(tweet_db_file)
    s = time.time()

    for i, user_chunk in enumerate(user_chunks):
        if int(ceil((float(i) / len(user_chunks))*100)) % 25 == 0 or i == len(user_chunks) - 1:
            print "\n\tProcessing chunk {} out of {}".format(i + 1, len(user_chunks)) 
        process_user_chunk(user_chunk, current_tweets_db, mover_tweets_db)

    current_tweets_db.connection.close()
    mover_tweets_db.connection.commit()
    mover_tweets_db.connection.close()

    print '\n\tElapsed Time for db file: {}s\n'.format(round(time.time() - s, 2))


if __name__ == '__main__':
    try: 
        chronology_db_file = fd.askopenfilename(title='Choose database with user chronology')
        tweets_db_files = fd.askopenfilenames(title='Choose databases with user tweets')

        if not chronology_db_file: 
            raise Exception('\nNo database selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()
    
    chronology_db = Database(chronology_db_file)

    if TEST_SIZE: 
        unique_users = [user[0] for user in chronology_db.select('SELECT DISTINCT user_id FROM user_chronology LIMIT {}'.format(TEST_SIZE))]
    else: 
        unique_users = [user[0] for user in chronology_db.select('SELECT DISTINCT user_id FROM user_chronology')]

    user_chunks = list(chunkify(unique_users, n=USER_CHUNK_SIZE))

    for i, tweet_db_file in enumerate(tweets_db_files):         
        print "\nProcessing tweets db {} out of {}".format(i + 1, len(tweets_db_files))
        process_db_file(user_chunks, tweet_db_file)
        
    print '\nElapsed Time: {}s\n'.format(round(time.time() - s, 2))
    print 'Size: {}\tUser Chunks: {}\n'.format(TEST_SIZE if TEST_SIZE else 'All', USER_CHUNK_SIZE)
