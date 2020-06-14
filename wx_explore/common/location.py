import math
import numpy

from wx_explore.common.models import Projection
from wx_explore.web.core import db


lut_meta = {}


def load_coordinate_lookup_meta(proj):
    lats = numpy.array(proj.lats)
    lons = numpy.array(proj.lons)

    return (lats, lons)


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


def clear_proj_cache():
    for k in lut_meta:
        del lut_meta[k]


def _dist(x, y, lat, lon, projlats, projlons):
    return math.sqrt((lat - projlats[y][x])**2 + (lon - projlons[y][x])**2)


def get_xy_for_coord(proj, coords):
    """
    Returns the x,y for a given (lat, lon) coordinate on the given projection
    """
    projlats, projlons = get_lookup_meta(proj)

    lat, lon = coords

    if not (projlons.min() <= lon <= projlons.max() and projlats.min() <= lat <= projlats.max()):
        return None

    x = proj.n_x // 2
    y = proj.n_y // 2

    # Dumb walk to figure out best x,y
    # Easier on memory than keeping kdtrees and not that much slower since we don't hit this very often
    while True:
        best = (None, None, None)  # dist, dx, dy

        for dx in [-1, 0, 1]:
            for dy in [-1, 0, 1]:
                dist = _dist(x + dx, y + dy, lat, lon, projlats, projlons)
                if best[0] is None or dist < best[0]:
                    best = (dist, dx, dy)

        if best[1] == 0 and best[2] == 0:
            break

        x = (x + best[1]) % proj.n_x
        y = (y + best[2]) % proj.n_y

    return (x, y)
