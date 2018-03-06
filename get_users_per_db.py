from utils.modulars import save_unique_users_per_db
from utils.database import Database

import multiprocessing as mp 
import tkFileDialog as fd
import time, sys, os


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

def parallel_users_insert(db_files): 
    pool = mp.Pool(processes=mp.cpu_count())

    processes = [pool.apply_async(save_unique_users_per_db, args=(db_file,)) for db_file in db_files]

    for p in processes: 
        p.get()

def raw_users_insert(db_files): 
    for db_file in db_files: 
        save_unique_users_per_db(db_file)


if __name__ == "__main__":
    try: 
        dbs = fd.askopenfilenames(title='Get databases')[:TEST_SIZE]

        if not dbs: 
            raise Exception('\nNo databases selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()

    if METHOD: 
        parallel_users_insert(dbs)
    else:
        raw_users_insert(dbs)

    print '\nElapsed Time: {}s\n'.format(round(time.time() - s, 2))
    print 'Method: {}, Size: {}\n'.format(METHOD_STR, TEST_SIZE if TEST_SIZE else 'All')
