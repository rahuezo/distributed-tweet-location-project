from utils.separation import chunkify
from utils.database import Database
from utils.configuration import DISTRIBUTED_TWEET_STATS_PATH, TWEET_STATS_COLUMNS
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
        tweet_stats_db_file = fd.askopenfilename(title='Choose database with tweet statistics')

        if not tweet_stats_db_file: 
            raise Exception('\nNo database selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()

    tweet_stats_db = Database(tweet_stats_db_file)

    unique_fips = sorted([str(fips[0]).zfill(5) for fips in tweet_stats_db.select('SELECT DISTINCT fips FROM statistics')])

    fips_chunks = list(chunkify(unique_fips, n=NCHUNKS_FIPS))

    pool = mp.Pool(processes=mp.cpu_count())
    processes = [pool.apply_async(process_fips_chunk, args=(chunk, tweet_stats_db_file)) for chunk in fips_chunks]

    total_time = 0 

    for i, p in enumerate(processes): 
        print '{} out of {} chunks'.format(i + 1, len(processes))
        loop_t = time.time()
        p.get()
        elapsed = round(time.time() - loop_t, 2)

        total_time += elapsed
        avg = round(total_time / (i + 1), 2)

        print '\tElapsed Time: {}s\tAvg. Time: {}s\n'.format(elapsed, avg)
    tweet_stats_db.connection.close()
        
    print '\nTotal Elapsed Time: {}s\n'.format(round(time.time() - s, 2))
