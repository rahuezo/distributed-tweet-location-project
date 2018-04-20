from utils.database import Database

import tkFileDialog as fd
import time
import sys
import os


if __name__ == "__main__":
    try: 
        dbs = fd.askopenfilenames(title='Get databases')
        if not dbs: 
            raise Exception('\nNo directory selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    s = time.time()

    for i, db_file in enumerate(dbs): 
        print "Indexing db {} out of {}".format(i + 1, len(dbs))
        
        db = Database(db_file)
        db.cursor.execute('CREATE INDEX IF NOT EXISTS user_idx ON tweets (user_id)')

    print '\nElapsed Time: {}s\n'.format(round(time.time() - s, 2))