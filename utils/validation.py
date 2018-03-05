def is_mover(locations): 
    return False if len(locations) == 1 else (len(locations) == len(set(locations)))
