from scipy.spatial import cKDTree
import functools
import numpy

from wx_explore.common.models import (
    Projection,
)
from wx_explore.web import app, db


lut_meta = {}


def load_coordinate_lookup_meta(proj):
    lats = numpy.array(proj.lats)
    lons = numpy.array(proj.lons)

    # GFS (and maybe others) have lons that range 0-360 instead of -180 to 180.
    # If found, transform them to match the standard range.
    if lons.max() > 180:
        lons = numpy.vectorize(lambda n: n if 0 <= n < 180 else n-360)(lons)

    tree = cKDTree(numpy.stack([lons.ravel(), lats.ravel()], axis=-1))
    return (lats, lons, tree)


def preload_coordinate_lookup_meta():
    """
    Preload all projection metadata for quick lookups
    """
    for proj in Projection.query.all():
        lut_meta[proj.id] = load_coordinate_lookup_meta(proj)


def get_lookup_meta(proj):
    if proj.id not in lut_meta:
        lut_meta[proj.id] = load_coordinate_lookup_meta(proj)
    return lut_meta[proj.id]


def get_xy_for_coord(proj, coords):
    """
    Returns the x,y for a given (lat, lon) coordinate on the given projection
    """
    projlats, projlons, tree = get_lookup_meta(proj)
    projshape = proj_shape(proj)

    lat, lon = coords
    if projlons.min() <= lon <= projlons.max() and projlats.min() <= lat <= projlats.max():
        idx = tree.query([lon, lat])[1]
        x = idx % projshape[1]
        y = idx // projshape[1]
        return (x, y)

    return None


def proj_shape(proj):
    """
    Returns the shape of the given projection, utilizing the in-memory cache
    to avoid a lot of DB traffic.
    """
    lats, lons, _ = get_lookup_meta(proj)
    assert lats.shape == lons.shape
    return lats.shape
