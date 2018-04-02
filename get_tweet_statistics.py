from utils.configuration import TWEET_STATS_DB, TWEET_STATS_TB, TWEET_STATS_COLUMNS
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


def process_user_chunk(user_chunk, weather_info_db): 
    stats = set()

    for user_id in user_chunk: 
        stats.add(get_tweet_distribution(weather_info_db, user_id))
    return stats


if __name__ == '__main__':
    try: 
        weather_info_db_files = fd.askopenfilenames(title='Choose databases with weather info tweets')

        if not weather_info_db_files: 
            raise Exception('\nNo database selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()

    stats_db = Database(TWEET_STATS_DB)
    stats_tb = stats_db.create_table(TWEET_STATS_TB, TWEET_STATS_COLUMNS)

    total_time = 0 

    for i, weather_info_db_file in enumerate(weather_info_db_files):
        print "Processing db file {} out of {}".format(i + 1, len(weather_info_db_files))

        weather_info_db = Database(weather_info_db_file)

        unique_users = [user[0] for user in weather_info_db.select('SELECT DISTINCT user_id FROM tweets')]
        user_chunks = list(chunkify(unique_users, n=1000))
        
        stats_db.cursor.execute('BEGIN')

        file_st = time.time()

        for j, user_chunk in enumerate(user_chunks): 
            if int(ceil((float(j) / len(user_chunks))*100)) % 25 == 0 or j == len(user_chunks) - 1:
                print "\tProcessing chunk {} out of {}".format(j + 1, len(user_chunks))

            stats_db.insert('INSERT INTO {tb} VALUES(?, ?, ?, ?, ?)'.format(tb=stats_tb), 
                process_user_chunk(user_chunk, weather_info_db), many=True)  

        elapsed = round(time.time() - file_st, 2)

        total_time += elapsed

        print '\n\tElapsed Time: {}s\t\tAvg. Time: {}s\n'.format(elapsed, round(float(total_time) / (i + 1), 2))      
        print  
            
        stats_db.connection.commit()

        weather_info_db.connection.close()
    stats_db.connection.close()
        
    print '\nTotal Elapsed Time: {}s\n'.format(round(time.time() - s, 2))
