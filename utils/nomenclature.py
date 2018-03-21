from configuration import DATE_FROM_FILE_SIZE, PATH_SEP


def get_chunk_name(chunk): 
    return '{}.db'.format(chunk[0].split(PATH_SEP)[-1][:DATE_FROM_FILE_SIZE])
