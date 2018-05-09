from utils.separation import chunkify
from utils.database import Database
from utils.configuration import AGGREGATED_TWEET_DATA_COLUMNS
from utils.timing import str2date
from math import ceil

import tkFileDialog as fd
import multiprocessing as mp
import time
import sys, os

NCHUNKS_FIPS = 100


def process_fips_chunk(chunk, stats_db_file):
    db = Database(stats_db_file)

    chunk_dbfile = os.path.join(DISTRIBUTED_TWEET_STATS_PATH, 'fips_{}_{}.db'.format(chunk[0], chunk[-1]))
    chunk_db = Database(chunk_dbfile)

    stats_tb = chunk_db.create_table('statistics', TWEET_STATS_COLUMNS)

    chunk_db.cursor.execute('BEGIN')

    for fips in chunk:
        results = db.select('SELECT * FROM statistics WHERE fips={}'.format(fips))
        chunk_db.insert('INSERT INTO {} VALUES (?, ?, ?, ?, ?)'.format(stats_tb), results, many=True)

    chunk_db.connection.commit()
    chunk_db.connection.close()
    db.connection.close()


if __name__ == '__main__':
    try: 
        tweet_stats_db_files = fd.askopenfilenames(title='Choose databases with tweet statistics')
        county_weather_db_file = fd.askopenfilename(title='Choose database with county weather')

        if not tweet_stats_db_files and not county_weather_db_file: 
            raise Exception('\nNo databases selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()


    for i, stats_dbfile in enumerate(tweet_stats_db_files): 
        print "{} out of {} dbs".format(i + 1, len(tweet_stats_db_files))
        stats_db = Database(stats_dbfile)
        stats_db.create_table('aggregated_tweet_data', AGGREGATED_TWEET_DATA_COLUMNS)

        stats_db.cursor.execute('ATTACH "{}" as wdb'.format(county_weather_db_file))

        stats_db.cursor.execute('BEGIN')

    #  'user_id INT, date_text TEXT, fips INT, tmax REAL, prcp REAL, humidity REAL, total_tweets INT, weather_tweets INT,'
        stats_db.cursor.execute(
            """
            INSERT INTO aggregated_tweet_data
            SELECT 
            statistics.user_id, statistics.date_text, statistics.fips, 
            weather.tmax, weather.prcp, weather.hmd,
            statistics.total_tweets, statistics.weather_tweets
            FROM statistics
            INNER JOIN
            wdb.county_weather weather 
            ON weather.fips = SUBSTR('00000' || statistics.fips, -5) AND weather.date_text = statistics.date_text           
            """
        )


        stats_db.connection.commit()
        stats_db.connection.close()



        
    print '\nTotal Elapsed Time: {}s\n'.format(round(time.time() - s, 2))
