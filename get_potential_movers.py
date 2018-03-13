from utils.configuration import POTENTIAL_MOVERS_COLUMNS, POTENTIAL_MOVERS_TBNAME, USER_FIPS_TBNAME
from utils.separation import chunkify
from utils.database import Database
from utils.validation import is_potential_mover

import multiprocessing as mp 
import tkFileDialog as fd
import time
import sys
import os


if len(sys.argv) > 1:
    TEST_SIZE = int(sys.argv[1])
    if TEST_SIZE < 0: 
        TEST_SIZE = None
else: 
    TEST_SIZE = 100


if __name__ == '__main__':
    try:
        aggregated_data_db = fd.askopenfilename(title='Choose database with aggregated data')

        if not aggregated_data_db:
            raise Exception('\nNo database selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()

    aggregated_db = Database(aggregated_data_db)
    potential_movers_tb = aggregated_db.create_table(POTENTIAL_MOVERS_TBNAME, POTENTIAL_MOVERS_COLUMNS)

    unique_users = [user[0] for user in aggregated_db.select('SELECT DISTINCT user_id FROM {}'.format(USER_FIPS_TBNAME))][:TEST_SIZE]

    user_chunks = chunkify(unique_users, n=10000)

    for user_chunk in user_chunks:

        aggregated_db.cursor.execute('BEGIN')

        for user in user_chunk:
            if is_potential_mover(aggregated_db, user):
                aggregated_db.insert('INSERT INTO {tbn} VALUES(?)'.format(tbn=potential_movers_tb), (user,))

        aggregated_db.connection.commit()

    aggregated_db.connection.close()

    print '\nElapsed Time: {}s\n'.format(round(time.time() - s, 2))
