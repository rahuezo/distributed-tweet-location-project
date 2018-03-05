from configuration import TWEETS_TBNAME, TWEETS_COLUMNS, RESULTS_DIR_PATH
from nomenclature import get_chunk_name
from files import get_tweets_from_file
from database import Database
from separation import chunk_files_by_day

import os


def create_db_and_tweets_tb(db_name):
    db_path = os.path.join(RESULTS_DIR_PATH, db_name) 
    db = Database(db_path)
    tweets_tb = db.create_table(TWEETS_TBNAME, TWEETS_COLUMNS)
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


