from collections import Counter
from database import Database


def get_modal_location(locations): 
    data = Counter(locations)
    return max(locations, key=data.get)
