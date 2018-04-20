from configuration import N_CHUNKS, DATE_FROM_FILE_SIZE, PATH_SEP
from itertools import groupby
from natsort import natsorted

import os


def chunk_files_by_day(fpaths): 
    fpaths = natsorted(fpaths)
    for i, j in groupby(fpaths, key=lambda x: x.split(PATH_SEP)[-1][:DATE_FROM_FILE_SIZE]): 
        yield list(j)


def chunkify(iterable, n=N_CHUNKS): 
    """Yield successive n-sized chunks from iterable."""
    for i in xrange(0, len(iterable), n):
        yield iterable[i: i + n]


# import tkFileDialog as fd
# import os


# wd = fd.askdirectory(title='Choose dir with tweets')

# files = [os.path.join(wd, f) for f in os.listdir(wd)]

# c = 0 
# for chunk in chunk_files_by_day(files): 
#     print chunk
#     print 
#     if c > 3: 
#         break 

    