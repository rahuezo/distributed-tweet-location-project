from utils.separation import chunk_files_by_day
from utils.modulars import commit_chunk

import multiprocessing as mp 
import tkFileDialog as fd
import time 
import sys
import os

# 0 for raw, 1 for multiprocessing 

if len(sys.argv) > 1: 
    METHOD = int(sys.argv[1])
else: 
    METHOD = 0

if len(sys.argv) > 2: 
    TEST_SIZE = int(sys.argv[2])
    if TEST_SIZE < 0: 
        TEST_SIZE = None
else: 
    TEST_SIZE = 100

METHOD_STR = 'Parallel' if METHOD else 'Raw'


def raw_importing(chunks): 
    for chunk in chunks: 
        commit_chunk(chunk)

def parallel_importing(chunks, factor=1): 
    pool = mp.Pool(processes=mp.cpu_count()*factor)

    processes = [pool.apply_async(commit_chunk, args=(chunk,)) for chunk in chunks]
    
    for p in processes: 
        p.get()

if __name__ == '__main__':
    try: 
        wd = fd.askdirectory(title='Choose directory with tweets')
        files = [os.path.join(wd, f) for f in os.listdir(wd)][:TEST_SIZE]
        chunks = chunk_files_by_day(files)
    except Exception as e: 
        print '\nNo directory selected! Goodbye.\n'
        sys.exit()

    s = time.time()

    if METHOD: 
        parallel_importing(chunks)
    else:
        raw_importing(chunks)

    print '\nElapsed Time: {}s\n'.format(round(time.time() - s, 2))
    print 'Method: {}, Size: {}\n'.format(METHOD_STR, TEST_SIZE)

    
