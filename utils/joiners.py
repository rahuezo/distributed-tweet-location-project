from database import Database
from validation import is_mover

import tkFileDialog as fd
import time
import multiprocessing as mp


def get_user_locations(user_id, db_files): 
    user_locations = set()

    for db_file in db_files: 
        db = Database(db_file)
        db.select('SELECT tweet_location FROM tweets WHERE user_id={uid}'.format(uid=user_id))
        for location in db.cursor: 
            user_locations.add(location[0])
    db.connection.close()    
    return user_locations


if __name__ == "__main__":
    users = [265190144, 388459151, 2407950806L, 29379629, 2329340188L, 60829975, 2153231840L, 179451754, 2243507567L, 501358792, 383577929, 1717302079, 2206188373L, 2261728016L, 245644335, 2148204210L, 1112495810, 207530211, 407762380, 971330550]

    dbs = fd.askopenfilenames(title='Get databases')

    s = time.time()

    pool = mp.Pool(processes=mp.cpu_count())

    processes = [pool.apply_async(get_user_locations, args=(uid, dbs)) for uid in users]

    # for uid in users: 
    #     print get_user_locations(uid, dbs)

    for p in processes: 
        print p.get()

    print 'Elapsed Time: {}s'.format(round(time.time() - s, 2))

