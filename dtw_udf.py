"""Stan Salvador, and Philip Chan. “FastDTW: Toward accurate dynamic time warping in linear time and space.”
Intelligent Data Analysis 11.5 (2007): 561-580."""

from scipy.spatial.distance import euclidean
from fastdtw import fastdtw


def get_dtw_distance(array1, array2):
    distance, path = fastdtw(array1, array2, dist=euclidean)
    return distance


