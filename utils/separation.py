from configuration import N_CHUNKS, DATE_FROM_FILE_SIZE
from itertools import groupby

from natsort import natsorted

import os

N = natsorted(['fileA1.txt', 'fileA2.txt', 'fileA3.txt', 'fileC1.txt', 'fileA1.txt', 'fileA11.txt'])


def chunk_files_by_day(fpaths): 
    fpaths = natsorted(fpaths)

    for i, j in groupby(fpaths, key=lambda x: x[:DATE_FROM_FILE_SIZE]): 
        yield list(j)


def chunkify(iterable, n=N_CHUNKS): 
    """Yield successive n-sized chunks from iterable."""
    for i in xrange(0, len(iterable), n):
        yield iterable[i: i + n]

import tkFileDialog as fd

wd = fd.askdirectory(title='Choose tweet files dir')

fpaths = [f for f in os.listdir(wd)]

c = 0 

for i in chunk_files_by_day(fpaths): 
    c += 1

print "Number of chunks: ", c
    