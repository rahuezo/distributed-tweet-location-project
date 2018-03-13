from collections import Counter
from haversine import haversine

import tkFileDialog as fd
import cPickle as pk


def get_centroids():
    centroids_file = fd.askopenfilename(title='Choose county_centroids.pkl')

    with open(centroids_file) as f:
        return pk.load(f)


def are_far_apart(centroids, fips1, fips2, distance=100):
    county1, county2 = centroids[str(fips1).zfill(5)], centroids[str(fips2).zfill(5)]
    return haversine(county1, county2, miles=True) > distance


def get_modal_location(locations): 
    data = Counter(locations)
    return max(locations, key=data.get)
