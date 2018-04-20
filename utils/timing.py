from datetime import datetime
import sys 


def format_date(date): 
    date = datetime.strptime(date, '%a %b %d %H:%M:%S +0000 %Y')
    return str(date.strftime('%Y-%m-%d %H:%M:%S'))


def str2date(s): 
    return datetime.strptime(s, '%Y-%m-%d')


def fips_date_mapper(user_chronology_db, user_id, date): 
    results = user_chronology_db.select("""SELECT fips1 FROM user_chronology
        WHERE user_id={uid} ORDER BY ABS(STRFTIME('%s', '{d}') - STRFTIME('%s', date1)) LIMIT 1""".format(d=date, uid=user_id)).fetchone()

    try: 
        return results[0]
    except Exception as e:     
        print "Error occurred ", e
        print user_id, date
        print 
        sys.exit()
        return None


