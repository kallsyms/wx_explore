from wx_explore.common.models import Metric
from wx_explore.common.db_utils import get_or_create


temp = get_or_create(Metric(
    name='2m Temperature',
    units='K',
))
visibility = get_or_create(Metric(
    name='Visibility',
    units='m',
))
raining = get_or_create(Metric(
    name='Rain',
    units='',
))
ice = get_or_create(Metric(
    name='Ice',
    units='',
))
freezing_rain = get_or_create(Metric(
    name='Freezing Rain',
    units='',
))
snowing = get_or_create(Metric(
    name='Snow',
    units='',
))
composite_reflectivity = get_or_create(Metric(
    name='Composite Reflectivity',
    units='dbZ',
))
humidity = get_or_create(Metric(
    name='2m Humidity',
    units='kg/kg',
))
pressure = get_or_create(Metric(
    name='Surface Pressure',
    units='Pa',
))
wind_u = get_or_create(Metric(
    name='10m Wind U-component',
    units='m/s',
    intermediate=True,
))
wind_v = get_or_create(Metric(
    name='10m Wind V-component',
    units='deg',
    intermediate=True,
))
wind_speed = get_or_create(Metric(
    name='10m Wind Speed',
    units='m/s',
))
wind_direction = get_or_create(Metric(
    name='10m Wind Direction',
    units='deg',
))
gust_speed = get_or_create(Metric(
    name='Gust Speed',
    units='m/s',
))
cloud_cover = get_or_create(Metric(
    name='Cloud Cover',
    units='%',
))

ALL_METRICS = [
    temp,
    visibility,
    raining,
    ice,
    freezing_rain,
    snowing,
    composite_reflectivity,
    humidity,
    pressure,
    wind_u,
    wind_v,
    wind_speed,
    wind_direction,
    gust_speed,
    cloud_cover,
]
