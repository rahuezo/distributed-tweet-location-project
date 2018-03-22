from datetime import datetime


def format_date(date): 
    date = datetime.strptime(date, '%a %b %d %H:%M:%S +0000 %Y')
    return str(date.strftime('%Y-%m-%d %H:%M:%S'))


def str2date(s): 
    return datetime.strptime(s, '%Y-%m-%d')
