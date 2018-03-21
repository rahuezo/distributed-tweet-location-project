from configuration import USER_ID_DB_NAME
from math import ceil

import os


def belongs_to(user_id, dbs): 
    pass


def get_user_id_range(db): 
    min_user_id, max_user_id = db.select('SELECT MIN(user_id), MAX(user_id) FROM movers').fetchone()
    return max_user_id - min_user_id


def get_user_id_ranges(user_id_range, n=100): 
    step = int(ceil(user_id_range / n))
    return [(i, i + step) for i in xrange(0, user_id_range, step)]


def get_dbname_from_range(current_range): 
    return USER_ID_DB_NAME.format(*current_range)


def get_user_db_names(user_id_range, n=100):
    step = int(ceil(user_id_range / n))
    return [USER_ID_DB_NAME.format(i, i + step) for i in xrange(0, user_id_range, step)]


def user_belongs(db_name, user_id): 
    min_user_id, max_user_id = db_name.split(os.sep)[-1].replace('.db', '').split('_')
    return min_user_id <= int(user_id) <= max_user_id


# print get_user_id_ranges(1234)

    
