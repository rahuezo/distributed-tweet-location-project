from utils.separation import chunkify
from utils.database import Database

from utils.timing import str2date
from math import ceil

import tkFileDialog as fd
import multiprocessing as mp
import time
import sys, os

NCHUNKS_FIPS = 100


if __name__ == '__main__':
    try: 
        tweet_stats_db_file = fd.askopenfilenames(title='Choose database with tweet statistics')

        if not tweet_stats_db_file: 
            raise Exception('\nNo database selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()

    tweet_stats_db = Database(tweet_stats_db_file)

    unique_fips = sorted([str(fips[0]).zfill(5) for fips in tweet_stats_db.select('SELECT DISTINCT fips FROM statistics')])

    fips_chunks = chunkify(unique_fips, n=NCHUNKS_FIPS)

    for fips_chunk in fips_chunks: 
        # process fips chunk
        # This can be parallelized because it's writing to different databases. 
        pass




    tweet_stats_db.connection.close()
        
    print '\nTotal Elapsed Time: {}s\n'.format(round(time.time() - s, 2))
