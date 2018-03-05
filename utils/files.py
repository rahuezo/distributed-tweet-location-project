from timing import format_date

import tkFileDialog as fd
import time, os, sys


def get_tweet_files(): 
    try: 
        wd = fd.askdirectory(title="Choose directory with tweet files")
        return map(lambda f: os.path.join(wd, f), filter(lambda f: f.endswith('.txt'), os.listdir(wd)))
    except: 
        print "No directory selected. Goodbye!"
        sys.exit()


def get_tweets_from_file(f, sep='<SEP>'): 
    with open(f, 'rb') as in_file: 
        for line in in_file: 
            try: 
                tweets = line.split(sep)
                tweets[-2] = format_date(tweets[-2])
                tweets[-1] = tweets[-1].strip()
                yield tweets
            except Exception: 
                continue


def get_users_from_file(f, sep='<SEP>', index=0): 
    with open(f, 'rb') as in_file: 
        for line in in_file: 
            yield line.split(sep)[index].strip()
