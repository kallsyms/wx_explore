import cv2
import numpy as np
import pygrib


def warp_flow(img, flow):
    h, w = flow.shape[:2]
    flow = -flow
    flow[:,:,0] += np.arange(w)
    flow[:,:,1] += np.arange(h)[:,np.newaxis]
    res = cv2.remap(img, flow, None, cv2.INTER_LINEAR)
    return res


def interpolate(msg_a, msg_b):
    """
    Interpolate values from GRIB message A to message B, in 1 minute steps.
    """
    dt = msg_b.forecastTime - msg_a.forecastTime

    frame_a, lat, lon = msg_a.data()
    frame_b = msg_b.values

    frame_a = frame_a.clip(min=0).astype(np.uint8)
    frame_b = frame_b.clip(min=0).astype(np.uint8)

    #frame_a = cv2.adaptiveThreshold(frame_a, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, 31, 0)
    #frame_b = cv2.adaptiveThreshold(frame_b, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, 31, 0)

    # TODO: param tuning
    flow = cv2.calcOpticalFlowFarneback(frame_a, frame_b, None, 0.3, 3, 15, 3, 5, 1.2, 0)

    # TODO: flow validation based on wind (i.e. flow direction should not significantly deviate from wind direction)

    # Create 2 series: one of a flowing to b, one of b flowing to a (-flow)
    # Final result will fade between the two of them

    a_to_b = [frame_a]
    for i in range(1, dt):
        a_to_b.append(warp_flow(a_to_b[i-1], flow/dt))

    b_to_a = [frame_b]
    for i in range(dt-1, 0, -1):
        b_to_a.insert(0, warp_flow(b_to_a[-1], (-flow)/dt))

    res = np.mean([a_to_b, b_to_a], axis=0)

    msgs = []

    # Dumb way to copy the message object
    msg_template = pygrib.fromstring(msg_a.tostring())

    for i, frame in enumerate(res):
        msg_template.values = frame.astype(np.float32)
        msg_template.forecastTime = msg_a.forecastTime + i
        msgs.append(pygrib.fromstring(msg_template.tostring()))

    return msgs


def _render(msg, out_filename):
    """
    Render data from GRIB message `msg` into a file `out_filename`.
    Mainly just for debugging.
    """
    import matplotlib.pyplot as plt
    from mpl_toolkits.basemap import Basemap

    frame, lat, lon = msg.data()

    m = Basemap(
            projection='mill',
            lat_ts=10,
            llcrnrlon=lon.min(),
            urcrnrlon=lon.max(),
            llcrnrlat=lat.min(),
            urcrnrlat=lat.max())

    ax, ay = m(lon, lat)

    m.pcolormesh(ax, ay, frame, shading='flat', cmap=plt.cm.jet)

    plt.tight_layout()
    plt.savefig(out_filename, dpi=300)
