from utils.modulars import save_unique_users_per_db
from utils.database import Database
from utils.configuration import USER_FIPS_TBNAME, USER_FIPS_DB, USER_FIPS_COLUMNS_UNIQUE, RESULTS_DIR_PATH
from datetime import datetime


import multiprocessing as mp 
import tkFileDialog as fd
import time, sys, os


def get_date_from_path(path): 
    return db_file.split('/')[-1][:-3].replace('_', '-')

if __name__ == "__main__":
    try: 
        dbs = fd.askopenfilenames(title='Get databases')

        if not dbs: 
            raise Exception('\nNo databases selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    aggregated_data_db_path = os.path.join(RESULTS_DIR_PATH, USER_FIPS_DB)     
    aggregated_data_db = Database(aggregated_data_db_path)

    user_fips_tb = aggregated_data_db.create_table(USER_FIPS_TBNAME, USER_FIPS_COLUMNS_UNIQUE)

    s = time.time()

    for db_file in dbs: 
        db = Database(db_file)

        db.select('SELECT * FROM {tbn}'.format(tbn=USER_FIPS_TBNAME))

        current_user_fips = [(user_id, fips, get_date_from_path(db_file)) for user_id, fips in db.cursor]

        aggregated_data_db.cursor.execute('BEGIN')
        aggregated_data_db.insert('INSERT OR IGNORE INTO {tbn} VALUES (?, ?, ?)'.format(tbn=user_fips_tb), current_user_fips, many=True)        
        aggregated_data_db.connection.commit()

        db.connection.close()
    
    aggregated_data_db.connection.close()

    print '\nElapsed Time: {}s\n'.format(round(time.time() - s, 2))
