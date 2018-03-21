from utils.configuration import POTENTIAL_MOVERS_TBNAME, MOVERS_DB_NAME, MOVERS_TBNAME, MOVERS_COLUMNS
from utils.separation import chunkify
from utils.database import Database
from utils.locating import are_far_apart, get_centroids

import tkFileDialog as fd
import time
import sys


if len(sys.argv) > 1: 
    TEST_SIZE = int(sys.argv[1])
    if TEST_SIZE < 0: 
        TEST_SIZE = None
else: 
    TEST_SIZE = 100


def user_fips_compare(user, aggregated_db_file, centroids): 
    aggregated_db = Database(aggregated_db_file)
    aggregated_db.select('SELECT fips, tweet_date FROM users_fips WHERE user_id={uid}'.format(uid=user))

    fips_dates = [i for i in aggregated_db.cursor]

    actual_moves = set()

    for i in xrange(len(fips_dates)): 
        for j in xrange(i + 1, len(fips_dates)): 
            fips1, date1 = fips_dates[i]
            fips2, date2 = fips_dates[j]

            try: 
                if are_far_apart(centroids, fips1, fips2): 
                    actual_moves.add((user, fips1, date1, fips2, date2))
            except: 
                continue

    aggregated_db.connection.close()
    return actual_moves


if __name__ == '__main__':
    try: 
        aggregated_data_db = fd.askopenfilename(title='Choose database with aggregated data')
        centroids = get_centroids()

        if not aggregated_data_db: 
            raise Exception('\nNo database selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()
    
    aggregated_db = Database(aggregated_data_db)

    movers_db = Database(MOVERS_DB_NAME)
    movers_tb = movers_db.create_table(MOVERS_TBNAME, MOVERS_COLUMNS)

    potential_movers = [user[0] for user in aggregated_db.select('SELECT DISTINCT user_id FROM {}'.format(POTENTIAL_MOVERS_TBNAME))][:TEST_SIZE]
    user_chunks = chunkify(potential_movers, n=10000)
 
    for user_chunk in user_chunks:
        movers_db.cursor.execute('BEGIN') 
        
        for user in user_chunk: 
            actual_moves = user_fips_compare(user, aggregated_data_db, centroids)

            if actual_moves: 
                movers_db.insert('INSERT INTO {tbn} VALUES(?, ?, ?, ?, ?)'.format(tbn=movers_tb), actual_moves, many=True)

        movers_db.connection.commit()
            
    print '\nElapsed Time: {}s\n'.format(round(time.time() - s, 2))
    print 'Size: {}\n'.format(TEST_SIZE if TEST_SIZE else 'All')
