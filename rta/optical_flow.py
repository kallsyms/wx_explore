#!/usr/bin/env python3
import cv2
import numpy as np
import pygrib
import sys

g = pygrib.open(sys.argv[1])

t1 = 15
t2 = 30

adat, lat, lon = g.select(shortName='refc', forecastTime=t1)[0].data()
frame_a = adat
frame_b = g.select(shortName='refc', forecastTime=t2)[0].data()[0]

frame_a = frame_a.clip(min=0).astype(np.uint8)
frame_b = frame_b.clip(min=0).astype(np.uint8)

#frame_a = cv2.adaptiveThreshold(frame_a, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, 31, 0)
#frame_b = cv2.adaptiveThreshold(frame_b, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, 31, 0)

# TODO: param tuning
flow = cv2.calcOpticalFlowFarneback(frame_a, frame_b, None, 0.3, 3, 15, 3, 5, 1.2, 0)
u, v = flow.transpose((2,0,1))

def warp_flow(img, flow):
    h, w = flow.shape[:2]
    flow = -flow
    flow[:,:,0] += np.arange(w)
    flow[:,:,1] += np.arange(h)[:,np.newaxis]
    res = cv2.remap(img, flow, None, cv2.INTER_LINEAR)
    return res

res = warp_flow(frame_a, flow)

import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

m = Basemap(projection='mill',lat_ts=10,llcrnrlon=lon.min(),urcrnrlon=lon.max(),llcrnrlat=lat.min(),urcrnrlat=lat.max())
ax, ay = m(lon, lat)

# Show every 10th quiver
sx = np.arange(0, ax.shape[1], 10)
sy = np.arange(0, ay.shape[0], 10)
qpts = np.meshgrid(sy, sx)

m.pcolormesh(ax,ay,frame_a,shading='flat',cmap=plt.cm.jet)

# larger scale = smaller arrows
#m.quiver(ax[qpts], ay[qpts], u[qpts], v[qpts], scale=100)

plt.show()

m.pcolormesh(ax,ay,res,shading='flat',cmap=plt.cm.jet)

# larger scale = smaller arrows
#m.quiver(ax[qpts], ay[qpts], u[qpts], v[qpts], scale=100)

plt.show()
m.pcolormesh(ax,ay,frame_b,shading='flat',cmap=plt.cm.jet)

# larger scale = smaller arrows
#m.quiver(ax[qpts], ay[qpts], u[qpts], v[qpts], scale=100)

plt.show()
