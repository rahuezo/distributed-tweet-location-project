from utils.configuration import USER_CHRONOLOGY_DB, USER_CHRONOLOGY_TB, USER_CHRONOLOGY_COLUMNS, NUMBER_OF_UNIQUE_USERS
from utils.separation import chunkify
from utils.database import Database
from utils.locating import are_far_apart, get_centroids
from utils.splitting import get_user_id_ranges, get_user_id_range, get_user_db_names, user_belongs, get_dbname_from_range
from utils.validation import in_range

from utils.timing import str2date

import tkFileDialog as fd
import multiprocessing as mp
import time
import sys, os


def get_user_chronology(user_id, db, centroids): 
    fips_list = sorted([fips for fips in db.select('SELECT fips1, date1 FROM user_fips WHERE user_id={}'.format(user_id))], key=lambda x: str2date(x[1])) 
    
    home = fips_list[0]
    records = set(((user_id, ) + home,))

    for i in xrange(len(fips_list)): 
        for j in xrange(i + 1, len(fips_list)): 
            (fips1, date1), (fips2, date2) = fips_list[i], fips_list[j]

            if str2date(date1) < str2date(date2): 
                if are_far_apart(centroids, fips1, fips2): 
                    home = fips_list[j]
                    records.add((user_id, ) + home)
    return list(records)


def process_user_chunk_chronology(user_chunk, db, centroids):
    db.cursor.execute('BEGIN')

    for user_id in user_chunk:  
        db.insert('INSERT INTO user_chronology VALUES(?, ?, ?)', get_user_chronology(user_id, db, centroids), many=True)
    
    db.connection.commit()

if len(sys.argv) > 1: 
    TEST_SIZE = int(sys.argv[1])
    if TEST_SIZE < 0: 
        TEST_SIZE = None
else: 
    TEST_SIZE = 100


if __name__ == '__main__':
    try: 
        chronology_db_file = fd.askopenfilename(title='Choose database with user chronology')
        centroids = get_centroids()

        if not chronology_db_file: 
            raise Exception('\nNo database selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()
    
    chronology_db = Database(chronology_db_file)

    if TEST_SIZE: 
        unique_users = [user[0] for user in chronology_db.select('SELECT DISTINCT user_id FROM user_fips LIMIT {}'.format(TEST_SIZE))]
    else: 
        unique_users = [user[0] for user in chronology_db.select('SELECT DISTINCT user_id FROM user_fips')]

    print "Number of Unique Users: {}".format(len(unique_users))

    chronology_tb = chronology_db.create_table('user_chronology', USER_CHRONOLOGY_COLUMNS)

    user_chunks = list(chunkify(unique_users, n=100))

    for i, user_chunk in enumerate(user_chunks): 
        print "Processing chunk {} out of {}".format(i + 1, len(user_chunks))
        
        process_user_chunk_chronology(user_chunk, chronology_db, centroids)
          
    print '\nElapsed Time: {}s\n'.format(round(time.time() - s, 2))
    print 'Size: {}\n'.format(TEST_SIZE if TEST_SIZE else 'All')
