from utils.modulars import get_users_in_db, get_user_field, commit_user_fips
from utils.configuration import USER_FIPS_TBNAME, USER_FIPS_COLUMNS
from utils.separation import chunkify
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


def process_user_chunks(user_chunks, db): 
    for chunk in user_chunks:
        fips_pairs = [get_user_field(user_id, db, 'fips') for user_id in chunk]
        commit_user_fips(db, fips_pairs)


def process_single_db(db_file): 
    db = Database(db_file)
    users_fips_tb = db.create_table(USER_FIPS_TBNAME, USER_FIPS_COLUMNS)

    user_chunks = chunkify([user[0] for user in get_users_in_db(db_file)], n=10000)
    process_user_chunks(user_chunks, db)
    

if __name__ == '__main__':
    try: 
        dbs = fd.askopenfilenames(title='Get databases')[:TEST_SIZE]

        if not dbs: 
            raise Exception('\nNo databases selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()
    
    if METHOD: 
        pool = mp.Pool(processes=mp.cpu_count())

        processes = [pool.apply_async(process_single_db, args=(db_file,)) for db_file in dbs]
        
        for p in processes: 
            p.get()
    else: 
        for db_file in dbs: 
            process_single_db(db_file)

    print '\nElapsed Time: {}s\n'.format(round(time.time() - s, 2))
    print 'Method: {}, Size: {}\n'.format(METHOD_STR, TEST_SIZE if TEST_SIZE else 'All')
