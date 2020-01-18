from scipy.spatial import cKDTree
import numpy
import pickle

from wx_explore.common.models import (
    Projection,
)


lut_meta = {}


def load_coordinate_lookup_meta(proj):
    lats = numpy.array(proj.lats)
    lons = numpy.array(proj.lons)
    tree = pickle.loads(proj.tree)

    return (lats, lons, tree)


def get_lookup_meta(proj):
    if proj.id not in lut_meta:
        lut_meta[proj.id] = load_coordinate_lookup_meta(proj)
    return lut_meta[proj.id]


def preload_coordinate_lookup_meta():
    """
    Preload all projection metadata for quick lookups
    """
    for proj in Projection.query.all():
        get_lookup_meta(proj)


def get_xy_for_coord(proj, coords):
    """
    Returns the x,y for a given (lat, lon) coordinate on the given projection
    """
    projlats, projlons, tree = get_lookup_meta(proj)
    projshape = proj.shape()

    lat, lon = coords
    if projlons.min() <= lon <= projlons.max() and projlats.min() <= lat <= projlats.max():
        idx = tree.query([lon, lat])[1]
        x = idx % projshape[1]
        y = idx // projshape[1]
        return (x, y)

    return None
