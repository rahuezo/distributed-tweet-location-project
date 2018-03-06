from configuration import (TWEETS_TBNAME, TWEETS_COLUMNS, 
                            RESULTS_DIR_PATH, MOVERS_TBNAME,
                            MOVERS_COLUMNS, MOVERS_DB_NAME,
                            USERS_TBNAME, USERS_COLUMNS, 
                            USER_FIPS_TBNAME, USER_FIPS_COLUMNS
                        )

from nomenclature import get_chunk_name
from files import get_tweets_from_file
from database import Database
from separation import chunk_files_by_day, chunkify
from validation import is_mover
from joiners import get_user_locations


import os
import multiprocessing as mp


def create_db_and_movers_tb(db_name):
    db_path = os.path.join(RESULTS_DIR_PATH, db_name) 
    db = Database(db_path)
    movers_tb = db.create_table(MOVERS_TBNAME, MOVERS_COLUMNS)
    return db


def create_db_and_tweets_tb(db_name):
    db_path = os.path.join(RESULTS_DIR_PATH, db_name) 
    db = Database(db_path)
    tweets_tb = db.create_table(TWEETS_TBNAME, TWEETS_COLUMNS)
    return db

def create_users_tb(db_name):
    db_path = os.path.join(RESULTS_DIR_PATH, db_name) 
    db = Database(db_path)
    users_tb = db.create_table(USERS_TBNAME, USERS_COLUMNS)
    return db


def commit_tweets(f, db): 
    db.cursor.execute('BEGIN')
    for tweet in get_tweets_from_file(f):
        try: 
            db.insert('INSERT INTO {tb_name} VALUES (?, ?, ?, ?, ?)'.format(tb_name=TWEETS_TBNAME), tweet)
        except Exception as e: 
            print e 
            continue
    db.connection.commit()


def commit_chunk(chunk): 
    db_name = get_chunk_name(chunk)
    db = create_db_and_tweets_tb(db_name)
    for f in chunk:         
        commit_tweets(f, db)
    db.connection.close()

def commit_movers_chunk(chunk, movers_db, all_dbs): 
    # movers_db.cursor.execute('BEGIN')

    for user_id in chunk: 
        user_locations = get_user_locations(user_id, all_dbs)
        if is_mover(user_locations): 
            pass
            # try: 
            #     movers_db.insert('INSERT INTO {tb_name} VALUES (?)'.format(tb_name=MOVERS_TBNAME), user_id)
            # except Exception as e: 
            #     print e 
            #     continue
    # movers_db.connection.commit()
    

def get_movers(chunk, all_dbs): 
    pool = mp.Pool(processes=mp.cpu_count())

    processes = [pool.apply_async(get_user_locations, args=(user_id, all_dbs)) for user_id in chunk]



def get_locations_per_db(db, users): 
    db = Database(db)

    for user in users: 
        locations = db.select('SELECT tweet_location FROM tweets WHERE user_id={uid}'.format(uid=user))
        print user, [i for i in locations]
        
    db.connection.close()


def save_unique_users_per_db(db_file): 
    db = create_users_tb(db_file)
    users = [user for user in db.select('SELECT DISTINCT user_id FROM tweets')]

    db.cursor.execute('BEGIN')       
    db.insert('INSERT INTO {tbn} VALUES(?)'.format(tbn=USERS_TBNAME), users, many=True)
    db.connection.commit()
    db.connection.close()


def get_users_in_db(db_file): 
    db = Database(db_file)
    db.select('SELECT DISTINCT user_id FROM users')
    return db.cursor


def get_user_field(user_id, db, field):   
    fips = db.select('SELECT {what} FROM tweets WHERE user_id={uid} LIMIT 1'.format(what=field, uid=user_id)).fetchone()[0]
    return user_id, fips


def commit_user_fips(db, users_fips): 
    db.cursor.execute('BEGIN')
    db.cursor.executemany('INSERT INTO {tbn} VALUES (?, ?)'.format(tbn=USER_FIPS_TBNAME), users_fips)
    db.connection.commit()
   

    


    