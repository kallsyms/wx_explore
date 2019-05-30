#!/usr/bin/env python3
from netCDF4 import Dataset
from scipy.spatial import cKDTree
from PIL import Image
import copy
import matplotlib
matplotlib.use('TkAgg')
import numpy
import pyart.graph.cm  # Just for the NWSRef colormap
import pygrib
import scipy.ndimage

# Basically a singleton that all GriddedFields can use to share KDTrees
_KD_TREE_CACHE = {}

class Grid(object):
    def __init__(self, lats, lons, shape):
        self.lats = numpy.ravel(lats)
        self.lons = numpy.ravel(lons)
        self.shape = shape

    def __repr__(self):
        return f"<Grid shape={self.shape}>"

    @property
    def lats_lons(self):
        return numpy.array([self.lats, self.lons])

    @property
    def num_pairs(self):
        return len(self.lats)  # since lats and lons are stored unravelled, the length of either is how many pairs there are

    @property
    def pairs(self):
        return numpy.dstack([self.lats, self.lons])[0]

class RegularGrid(Grid):
    def __init__(self, lats, lons, lat_step=None, lon_step=None):
        all_lats = numpy.repeat(lats, len(lons))
        all_lons = numpy.tile(lons, len(lats))
        super().__init__(all_lats, all_lons, (len(lats), len(lons)))
        self.lat_step = lat_step or round(lats[1] - lats[0], 5)
        self.lon_step = lon_step or round(lons[1] - lons[0], 5)

    @staticmethod
    def from_ranges(lat_min, lat_max, lat_step, lon_min, lon_max, lon_step):
        lats = numpy.arange(lat_min, lat_max, lat_step)
        lons = numpy.arange(lon_min, lon_max, lon_step)
        return RegularGrid(lats, lons, lat_step, lon_step)


class GriddedField(object):
    def __init__(self, values, grid):
        self.grid = grid
        self.values = values
        if self.values.shape != grid.shape:
            self.values = self.values.reshape(grid.shape)
        self.lats = grid.lats
        self.lons = grid.lons
        self.shape = grid.shape

        self.kd_tree = _KD_TREE_CACHE.get(self.shape)

    @staticmethod
    def from_grib_msg(msg):
        """
        Construct a GriddedField from the given pygrib message
        """
        lats, lons = msg.latlons()
        gf = GriddedField(msg.values, Grid(lats, lons, msg.values.shape))
        return gf

    def k_nearest_points(self, coord_pairs, **kwargs):
        """
        Find 1 or more indexes into our values that are closest to the given lat,lon pairs.
        """
        if self.kd_tree is None:
            self.kd_tree = cKDTree(numpy.dstack([self.lats, self.lons])[0])
            _KD_TREE_CACHE[self.shape] = self.kd_tree

        idxs = self.kd_tree.query(coord_pairs, **kwargs)[1]
        if type(idxs) is int:  # if k=1, query returns a single int. Make it a list for the comprehension below
            idxs = [idxs]

        return numpy.array([(idx // self.shape[1], idx % self.shape[1]) for idx in idxs])

    def nearest_point(self, lat, lon):
        """
        Find the x,y indexes into the given message's values that is closest to the given lat, lon
        """
        return self.k_nearest_points([(lat, lon)], k=1)[0]

    def resample_to_grid(self, dst_grid):
        """
        Resample to a standard grid
        """
        grid_idxs = self.k_nearest_points(dst_grid.pairs, k=1)
        # indexing expects [[x1, x2, x3], [y1, y2, y3]] so transpose [[x1, y1], [x2, y2]] to the expected form
        grid_idxs = grid_idxs.transpose()
        grid_vals = self.values[grid_idxs[0], grid_idxs[1]]
        return GriddedField(grid_vals, dst_grid)

    def zoom(self, dst_grid):
        zoomed_values = scipy.ndimage.zoom(self.values, (dst_grid.shape[0]/self.shape[0], dst_grid.shape[1]/self.shape[1]))
        return GriddedField(zoomed_values, dst_grid)

    def render(self, file_path, colormap=matplotlib.cm.gist_ncar):
        # N.B. This must be an int dtype otherwise colormap thinks this is between 0 and 1
        # But int8 doesn't work???
        vals = numpy.int16(self.values)
        cm = copy.copy(colormap)
        cm.set_under(alpha=0)
        img = Image.fromarray(numpy.uint8(cm(vals)*255), 'RGBA')
        img.save(file_path)


class WindSkewProjector(object):
    def __init__(self, target_field, u_wind_field, v_wind_field, step_duration):
        self.target_field = target_field
        self.u_wind_field = u_wind_field
        self.v_wind_field = v_wind_field
        self.step_duration = step_duration

        if not (self.target_field.shape == self.u_wind_field.shape == self.v_wind_field.shape):
            raise ValueError("All fields must have the same shape")

        self.grid = self.target_field.grid

        # Our starting array is the target
        self.values = self.target_field.values
        self.coord_map = self._compute_step_indexes()

    def _compute_step_indexes(self):
        """
        For each index, compute the index whose value will be calculated to determine what value this index should hold
        after a step.

        For example, if our target field is a grid with 0.01deg spacing, the U vector wind field has an intensity of
        0.01deg/minute, there's no V wind, and our step_duration is 60 (seconds) then we'll return
        [[(0, 0), (0, 0), (0, 1)],
         [(1, 0), (1, 0), (1, 1)],
         [(2, 0), (2, 0), (2, 1)]]

        (but in the shape of grid.lats_lons - i.e. all lat idxs, then all lon idxs)

        In that case, the longitudinal index (the second one) has been shifted back by 1 for each index pair.

        Then step() interpolates these and produces a new .values array, where in this case, all values have shifted
        east by a single element.
        """
        earth_radius = 6378 * 1000
        m_per_deg = 2*numpy.pi*earth_radius/360  # TODO: This should really be (earth_radius+height)

        # new_idxs is all lat indexes, then all lon indexes (like grid.lats_lons)
        new_idxs = numpy.empty([2, self.grid.num_pairs])

        # For each unique latitude
        for idx, lat in enumerate(self.grid.lats[::self.grid.shape[1]]):
            # First, convert the m/s wind fields into deg/s
            # U is parallel with the equator, so latitude must be taken into account
            u_wind_dps = self.u_wind_field.values[idx]/m_per_deg * numpy.cos(numpy.radians(lat))
            v_wind_dps = self.v_wind_field.values[idx]/m_per_deg

            # Multiply by duration
            u_wind = u_wind_dps * self.step_duration
            v_wind = v_wind_dps * self.step_duration

            # Then scale by the size of the grid (1 index is not 1 degree, but 1/grid_step degrees)
            u_wind *= 1/self.grid.lon_step
            v_wind *= 1/self.grid.lat_step

            # Then _subtract_ the grid offset due to wind. Subtract because we're trying to
            # find where each index will get its next value from, not where a value is going to
            new_lon_idx = numpy.arange(0, self.grid.shape[1]) - u_wind
            new_lat_idx = idx - v_wind

            # Clamp it to be in a valid range so we don't wrap around or go oob
            new_lon_idx = new_lon_idx.clip(0, self.grid.shape[1])
            new_lat_idx = new_lat_idx.clip(0, self.grid.shape[0])

            new_idxs[0][idx*self.grid.shape[1] : (idx+1)*self.grid.shape[1]] = new_lat_idx
            new_idxs[1][idx*self.grid.shape[1] : (idx+1)*self.grid.shape[1]] = new_lon_idx

        return new_idxs

    def step(self):
        self.values = scipy.ndimage.map_coordinates(self.values, self.coord_map).reshape(self.grid.shape)
        return GriddedField(self.values, self.grid)


rad_ds = Dataset('/Users/nickgregory/Downloads/nc-ignore/wx/rta_testing/n0q_comp.nc')
rad_lats, rad_lons, rad_data = (rad_ds.variables.get(k)[...] for k in ('lat', 'lon', 'composite_n0q'))
rad_data.set_fill_value(-40)
rad = GriddedField(rad_data.filled(), RegularGrid(rad_lats, rad_lons))
print("Loaded radar composite")

hrrr_grib = pygrib.open('/Users/nickgregory/Downloads/nc-ignore/wx/rta_testing/hrrr.t01z.wrfsubhf01.grib2')
u_wind = GriddedField.from_grib_msg(hrrr_grib.select(name='U component of wind')[0])
print("Loaded U wind")
v_wind = GriddedField.from_grib_msg(hrrr_grib.select(name='V component of wind')[0])
print("Loaded V wind")

small_grid = RegularGrid.from_ranges(
    rad_lats[0], rad_lats[-1], 0.03,
    rad_lons[0], rad_lons[-1], 0.03,
)

u_wind_resampled = u_wind.resample_to_grid(small_grid)
print("Resampled U wind")
v_wind_resampled = v_wind.resample_to_grid(small_grid)
print("Resampled V wind")

u_wind_zoomed = u_wind_resampled.zoom(rad.grid)
print("Zoomed U wind")
v_wind_zoomed = v_wind_resampled.zoom(rad.grid)
print("Zoomed V wind")

STEP_DURATION = 60  # seconds
projector = WindSkewProjector(rad, u_wind_zoomed, v_wind_zoomed, STEP_DURATION)
for i in range(15):
    forecast = projector.step()
    print(f"Step {i}")
    forecast.render(f"{i}.png", pyart.graph.cm.NWSRef)
