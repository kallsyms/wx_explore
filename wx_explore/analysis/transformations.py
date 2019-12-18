import numpy


def derive_wind(u, v):
    """
    Derives wind speed and direction (relative to north) from U,V components
    """
    c = u + v*1j
    speed = numpy.abs(c)
    angle = numpy.angle(c, deg=True)
    # Convert angle relative to the x-axis to a north-relative angle
    angle -= 90
    angle = -angle % 360
    return speed, angle
