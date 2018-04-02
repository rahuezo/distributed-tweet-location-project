from datetime import datetime
import sys 


def format_date(date): 
    date = datetime.strptime(date, '%a %b %d %H:%M:%S +0000 %Y')
    return str(date.strftime('%Y-%m-%d %H:%M:%S'))


def str2date(s): 
    return datetime.strptime(s, '%Y-%m-%d')


def fips_date_mapper(user_chronology_db, user_id, date): 
    results = user_chronology_db.select("""SELECT fips1 FROM user_chronology
        WHERE date1 <= '{d}' AND user_id={uid} ORDER BY date1 DESC LIMIT 1""".format(d=date, uid=user_id)).fetchone()

    try: 
        return results[0]
    except:     
        print "Error occurred"
        print user_id, date
        print 
        sys.exit()
        return None


