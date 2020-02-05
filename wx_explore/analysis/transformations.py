import numpy


def cartesian_to_polar(u, v):
    """
    Transforms U,V into r,theta, with theta being relative to north (instead of east, a.k.a. the x-axis).
    Mainly for wind U,V to wind speed,direction transformations.
    """
    c = u + v*1j
    r = numpy.abs(c)
    theta = numpy.angle(c, deg=True)
    # Convert angle relative to the x-axis to a north-relative angle
    theta -= 90
    theta = -theta % 360
    return r, theta
