#!/usr/bin/env python3
import cv2
import numpy as np
import pygrib
import sys
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

g = pygrib.open(sys.argv[1])

t1 = 15
t2 = 30

dt = t2-t1

frame_a, lat, lon = g.select(shortName='refc', forecastTime=t1)[0].data()
frame_b, _, _ = g.select(shortName='refc', forecastTime=t2)[0].data()

frame_a = frame_a.clip(min=0).astype(np.uint8)
frame_b = frame_b.clip(min=0).astype(np.uint8)

#frame_a = cv2.adaptiveThreshold(frame_a, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, 31, 0)
#frame_b = cv2.adaptiveThreshold(frame_b, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, 31, 0)

# TODO: param tuning
flow = cv2.calcOpticalFlowFarneback(frame_a, frame_b, None, 0.3, 3, 15, 3, 5, 1.2, 0)

def warp_flow(img, flow):
    h, w = flow.shape[:2]
    flow = -flow
    flow[:,:,0] += np.arange(w)
    flow[:,:,1] += np.arange(h)[:,np.newaxis]
    res = cv2.remap(img, flow, None, cv2.INTER_LINEAR)
    return res


# TODO: flow validation based on wind (i.e. flow direction should not significantly deviate from wind direction)

# Create 2 series: one of a flowing to b, one of b flowing to a (-flow)
# Final result will crossfade between the two of them

a_to_b = [frame_a]
for i in range(1, dt):
    a_to_b.append(warp_flow(a_to_b[i-1], flow/dt))

b_to_a = [frame_b]
for i in range(dt-1, 0, -1):
    b_to_a.insert(0, warp_flow(b_to_a[-1], (-flow)/dt))

res = np.mean([a_to_b, b_to_a], axis=0)


m = Basemap(projection='mill',lat_ts=10,llcrnrlon=lon.min(),urcrnrlon=lon.max(),llcrnrlat=lat.min(),urcrnrlat=lat.max())
ax, ay = m(lon, lat)

# Show every 10th quiver
sx = np.arange(0, ax.shape[1], 10)
sy = np.arange(0, ay.shape[0], 10)
qpts = np.meshgrid(sy, sx)

#for i, frame in enumerate(res):
#    m.pcolormesh(ax,ay,frame,shading='flat',cmap=plt.cm.jet)

#    # larger scale = smaller arrows
#    #m.quiver(ax[qpts], ay[qpts], u[qpts], v[qpts], scale=100)

#    plt.tight_layout()
#    plt.savefig(f'{i}.png', dpi=300)
