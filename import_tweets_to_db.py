from utils.separation import chunk_files_by_day
from utils.modulars import commit_chunk

import multiprocessing as mp 
import tkFileDialog as fd
import time 
import os


METHOD = 1 # 0 for raw, 1 for multiprocessing 


def raw_importing(chunks): 
    for chunk in chunks: 
        commit_chunk(chunk)

def parallel_importing(chunks, factor=1): 
    pool = mp.Pool(processes=mp.cpu_count()*factor)

    processes = [pool.apply_async(commit_chunk, args=(chunk,)) for chunk in chunks]
    
    for p in processes: 
        p.get()


if __name__ == "__main__":
    wd = fd.askdirectory(title='Choose directory with tweets')

    files = [os.path.join(wd, f) for f in os.listdir(wd)][:100]
    chunks = chunk_files_by_day(files)

    s = time.time()

    if METHOD: 
        parallel_importing(chunks)
    else:
        raw_importing(chunks)

    print "Elapsed Time: {}s".format(round(time.time() - s, 2))
