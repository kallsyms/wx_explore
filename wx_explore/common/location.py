from scipy.spatial import cKDTree
import functools
import numpy

from wx_explore.common.models import (
    SourceField,
    Projection,
)
from wx_explore.web import db


lut_meta = {}


def get_coordinate_lookup_meta(proj):
    lats, lons = proj.latlons
    lats = numpy.array(lats)
    lons = numpy.array(lons)

    # GFS (and maybe others) have lons that range 0-360 instead of -180 to 180.
    # If found, transform them to match the standard range.
    if lons.max() > 180:
        lons = numpy.vectorize(lambda n: n if 0 <= n < 180 else n-360)(lons)

    latmin = lats.min()
    latmax = lats.max()
    lonmin = lons.min()
    lonmax = lons.max()

    tree = cKDTree(numpy.dstack([lons.ravel(), lats.ravel()])[0])
    return (lats, lons, tree)


def preload_coordinate_lookup_meta():
    """
    Preload all projection metadata for quick lookups
    """
    for proj in Projection.query.all():
        lut_meta[proj.id] = get_coordinate_lookup_meta(proj)


def get_lookup_meta(proj):
    if proj.id not in lut_meta:
        lut_meta[proj.id] = get_coordinate_lookup_meta(proj)
    return lut_meta[proj.id]


def get_xy_for_coord(source_field_id, coords):
    """
    Returns the proj_id,x,y for a given (lat, lon) coordinate
    """
    if lut_meta is None:
        preload_coordinate_lookup_meta()

    sf = SourceField.query.get(source_field_id)
    if sf is None:
        raise ValueError("No such source field")

    projlats, projlons, tree = get_lookup_meta(sf.projection)

    lat, lon = coords
    if lons.min() <= lon <= lons.max() and lats.min() <= lat <= lats.max():
        idx = tree.query([lon, lat])[1]
        x = idx % len(lons)
        y = idx // len(lons)
        return (sf.projection_id, x, y)

    return None


