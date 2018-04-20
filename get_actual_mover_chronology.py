from utils.configuration import USER_CHRONOLOGY_DB, USER_CHRONOLOGY_TB, USER_CHRONOLOGY_COLUMNS, NUMBER_OF_UNIQUE_USERS
from utils.separation import chunkify
from utils.database import Database
from utils.locating import are_far_apart, get_centroids
from utils.splitting import get_user_id_ranges, get_user_id_range, get_user_db_names, user_belongs, get_dbname_from_range
from utils.validation import in_range

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


def create_user_db(user_db): 
    db = Database(user_db)
    movers_tb = db.create_table(USER_ID_TBNAME, USER_ID_COLUMNS)
    db.connection.close()


def parallel_user_dbs(uid_range): 
    pool = mp.Pool(processes=mp.cpu_count())

    processes = [pool.apply_async(create_user_db, args=(user_db,)) for user_db in get_user_db_names(uid_range)]

    for p in processes: 
        p.get()

if __name__ == '__main__':
    try: 
        actual_movers_db = fd.askopenfilename(title='Choose database with actual movers')

        if not actual_movers_db: 
            raise Exception('\nNo database selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()
    
    movers_db = Database(actual_movers_db)

    movers_db.select("""SELECT DISTINCT user_id, fips1, date1 FROM movers 
            UNION SELECT DISTINCT user_id, fips2, date2 FROM movers ORDER BY user_id""")

    chronology_db = Database(USER_CHRONOLOGY_DB)
    chronology_tb = chronology_db.create_table(USER_CHRONOLOGY_TB, USER_CHRONOLOGY_COLUMNS)

    while True:         
        fips_rows = movers_db.cursor.fetchmany(1000)
        if not fips_rows: 
            print "There are no more rows to process.\n"
            break 
        chronology_db.cursor.execute('BEGIN')
        chronology_db.insert('INSERT INTO {tbn} VALUES(?, ?, ?)'.format(tbn=chronology_tb), fips_rows, many=True)
        chronology_db.connection.commit() 
        print "Inserted row.\n"
    
    # uid_range = get_user_id_range(movers_db)
    
    # fips_rows_chunks = [(get_dbname_from_range(user_range), filter(lambda x: in_range(x[0], user_range), fips_rows)) for user_range in get_user_id_ranges(uid_range)]

    # for chunk in fips_rows_chunks: 
    #     dbname, rows = chunk

    #     if len(rows) > 0: 
    #         db = Database(dbname)
    #         movers_tb = db.create_table(USER_ID_TBNAME, USER_ID_COLUMNS)

    #         db.cursor.execute('BEGIN')
    #         db.insert('INSERT INTO {tbn} VALUES(?, ?, ?)'.format(tbn=movers_tb), rows, many=True)
    #         db.connection.commit()
    #         db.connection.close()


    print '\nElapsed Time: {}s\n'.format(round(time.time() - s, 2))
    print 'Size: {}\n'.format(TEST_SIZE if TEST_SIZE else 'All')
