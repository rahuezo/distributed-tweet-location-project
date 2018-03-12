from utils.joiners import get_unique_users, get_user_locations
from utils.configuration import MOVERS_DB_NAME, RESULTS_DIR_PATH, MOVERS_TBNAME
from utils.separation import chunkify
from utils.modulars import commit_movers_chunk, create_db_and_movers_tb, get_movers, get_locations_per_db
from utils.database import Database
from utils.locating import get_user_location

import multiprocessing as mp 
import tkFileDialog as fd
import time
import sys
import os


# 0 for raw, 1 for multiprocessing 

if len(sys.argv) > 1: 
    METHOD = int(sys.argv[1])
else: 
    METHOD = 0

if len(sys.argv) > 2: 
    TEST_SIZE = int(sys.argv[2])
    if TEST_SIZE < 0: 
        TEST_SIZE = None
else: 
    TEST_SIZE = 100

METHOD_STR = 'Parallel' if METHOD else 'Raw'


def raw_save_potential_movers(chunks, all_dbs):  
    db_path = os.path.join(RESULTS_DIR_PATH, MOVERS_DB_NAME)
    db = create_db_and_movers_tb(db_path)

    for chunk in chunks: 
        commit_movers_chunk(chunk, db, all_dbs)
    db.connection.close()

def parallel_save_potential_movers(chunks, all_dbs): 
    db_path = os.path.join(RESULTS_DIR_PATH, MOVERS_DB_NAME)
    db = create_db_and_movers_tb(db_path)

    pool = mp.Pool(processes=mp.cpu_count())

    processes = [pool.apply_async(commit_movers_chunk, args=(chunk, db, all_dbs)) for chunk in chunks]

    for p in processes: 
        p.get()
    db.connection.close()



if __name__ == '__main__':
    try: 
        dbs = fd.askopenfilenames(title='Get databases')
        if not dbs: 
            raise Exception('\nNo directory selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()

    for db_file in dbs: 
        db = Database(db_file)
        unique_users = [user for user in db.select('SELECT user_id FROM users')]

        pool = mp.Pool(processes=mp.cpu_count())

        processes = [pool.apply_async(get_user_location, args=(uid, db_file)) for uid in unique_users]
        
        movers_tb = db.create_table(MOVERS_TBNAME, 'user_id INT, tweet_location TEXT')

        db.cursor.execute('BEGIN')

        for p in processes: 
            db.insert('INSERT INTO {tbn} VALUES(?, ?)'.format(tbn=movers_tb), p.get())
        
        db.connection.commit()
        db.connection.close()

        # get_locations_per_db(db, unique_users)

    # if METHOD: 
    #     parallel_save_potential_movers(chunked_users, dbs)
    # else:
    #     raw_save_potential_movers(chunked_users, dbs)

    print '\nElapsed Time: {}s\n'.format(round(time.time() - s, 2))
    print 'Method: {}, Size: {}\n'.format(METHOD_STR, TEST_SIZE)