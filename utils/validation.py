def is_mover(locations): 
    return False if len(locations) == 1 else (len(locations) == len(set(locations)))

def is_potential_mover(db, user_id, cnt=1): 
    count = int(db.select('SELECT COUNT(user_id) FROM users_fips WHERE user_id={uid}'.format(uid=user_id)).fetchone()[0]) 
    return count > cnt