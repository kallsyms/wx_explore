#!/usr/bin/env python3
import pygrib
import openpiv.tools
import openpiv.process
import openpiv.scaling
import openpiv.validation
import openpiv.filters
import numpy as np
import sys

g = pygrib.open(sys.argv[1])

t1 = 15
t2 = 60

adat, lat, lon = g.select(shortName='refc', forecastTime=t1)[0].data()
frame_a = adat
frame_b = g.select(shortName='refc', forecastTime=t2)[0].data()[0]

frame_a[frame_a < 25] = np.nan
frame_b[frame_b < 25] = np.nan

u, v, sig2noise = openpiv.process.extended_search_area_piv(
    frame_a.astype(np.int32),
    frame_b.astype(np.int32),
    window_size=24,
    overlap=12,
    dt=(t2-t1) * 60,
    search_area_size=64,
    sig2noise_method='peak2peak'
)

u, v, mask = openpiv.validation.sig2noise_val(u, v, sig2noise, threshold = 1.0)

u, v, mask = openpiv.validation.global_val(u, v, (-1000, 2000), (-1000, 1000))

# u, v = openpiv.filters.replace_outliers(u, v, method='localmean', max_iter=10, kernel_size=2)

from skimage.transform import resize
u = resize(u, lat.shape, anti_aliasing=True)
v = resize(v, lat.shape, anti_aliasing=True)

u[np.isnan(u)] = 0
v[np.isnan(v)] = 0

import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

m = Basemap(projection='mill',lat_ts=10,llcrnrlon=lon.min(),urcrnrlon=lon.max(),llcrnrlat=lat.min(),urcrnrlat=lat.max())
ax, ay = m(lon, lat)
m.pcolormesh(ax,ay,adat,shading='flat',cmap=plt.cm.jet)

sx = np.arange(0, ax.shape[1], 10)
sy = np.arange(0, ay.shape[0], 10)
pts = np.meshgrid(sy, sx)

m.quiver(ax[pts], ay[pts], u[pts], v[pts], scale=100000)

plt.show()
