from datetime import datetime, timedelta
from flask import Blueprint, abort, jsonify, request

import collections
import sqlalchemy

from wx_explore.analysis import (
    combine_models,
    cluster,
    SummarizedData,
    TemperatureEvent,
    PrecipEvent,
    WindEvent,
)
from wx_explore.common.models import (
    Source,
    SourceField,
    Location,
    Metric,
)
from wx_explore.common.storage import load_data_points
from wx_explore.common.utils import datetime2unix
from wx_explore.web import app


api = Blueprint('api', __name__, url_prefix='/api')


@api.route('/sources')
def get_sources():
    """
    Get all sources that data points can come from.
    :return: List of sources.
    """
    res = []

    for source in Source.query.all():
        j = source.serialize()
        j['fields'] = [f.serialize() for f in source.fields]
        res.append(j)

    return jsonify(res)


@api.route('/source/<int:src_id>')
def get_source(src_id):
    """
    Get data about a specific source.
    :param src_id: The ID of the source.
    :return: An object representing the source.
    """
    source = Source.query.get_or_404(src_id)

    j = source.serialize()
    j['fields'] = [f.serialize() for f in source.fields]

    return jsonify(j)


@api.route('/metrics')
def get_metrics():
    """
    Get all metrics that data points can be.
    :return: List of metrics.
    """
    return jsonify([m.serialize() for m in Metric.query.all()])


@api.route('/location/search')
def get_location_from_query():
    """
    Search locations by name prefix.
    :return: A list of locations matching the search query.
    """
    search = request.args.get('q')

    if search is None or len(search) < 2:
        abort(400)

    # Fixes basic weird results that could come from users entering '\'s, '%'s, or '_'s
    search = search.replace('\\', '\\\\').replace('_', '\_').replace('%', '\%')
    search = search.replace(',', '')
    search = search.lower()

    query = Location.query \
            .filter(sqlalchemy.func.lower(sqlalchemy.func.replace(Location.name, ',', '')).like('%' + search + '%')) \
            .order_by(Location.population.desc().nullslast()) \
            .limit(10)

    return jsonify([l.serialize() for l in query.all()])


@api.route('/location/by_coords')
def get_location_from_coords():
    """
    Get the nearest location from a given lat, lon.
    :return: The location.
    """

    lat = float(request.args['lat'])
    lon = float(request.args['lon'])

    if lat > 90 or lat < -90 or lon > 180 or lon < -180:
        abort(400)

    # TODO: may need to add distance limit if perf drops
    location = Location.query.order_by(Location.location.distance_centroid('POINT({} {})'.format(lon, lat))).first()

    return jsonify(location.serialize())


@api.route('/wx')
def wx_for_location():
    """
    Gets the weather for a specific location, optionally limiting by metric and time.
    at that time.
    """
    lat = float(request.args['lat'])
    lon = float(request.args['lon'])

    if lat > 90 or lat < -90 or lon > 180 or lon < -180:
        abort(400)

    requested_metrics = request.args.getlist('metrics', int)

    if requested_metrics:
        metric_ids = set(requested_metrics)
    else:
        metric_ids = Metric.query.with_entities(Metric.id)

    now = datetime.utcnow()
    start = request.args.get('start', type=int)
    end = request.args.get('end', type=int)

    if start is None:
        start = now - timedelta(hours=1)
    else:
        start = datetime.utcfromtimestamp(start)

        if not app.debug:
            if start < now - timedelta(days=1):
                start = now - timedelta(days=1)

    if end is None:
        end = now + timedelta(hours=12)
    else:
        end = datetime.utcfromtimestamp(end)

        if not app.debug:
            if end > now + timedelta(days=7):
                end = now + timedelta(days=7)

    requested_source_fields = SourceField.query.filter(
        SourceField.metric_id.in_(metric_ids),
        SourceField.projection_id != None,
    ).all()

    data_points = load_data_points((lat, lon), start, end, requested_source_fields)

    # valid time -> data points
    datas = collections.defaultdict(list)

    for fbm, values in data_points.items():
        datas[datetime2unix(fbm.valid_time)].append({
            'run_time': datetime2unix(fbm.run_time),
            'src_field_id': fbm.source_field_id,
            'value': values[0],  # TODO
        })

    wx = {
        'data': datas,
        'ordered_times': sorted(datas.keys()),
    }

    return jsonify(wx)


@api.route('/location/<int:loc_id>/wx/summarize')
def summarize(loc_id):
    """
    Summarizes the weather in a natural way.
    :param loc_id: The ID of the location to get weather for.
    :return: A list of strings summarizing the weather. One per day.
    """
    location = Location.query.get_or_404(loc_id)

    temp_sourcefield = SourceField.query(SourceField.source_id == , SourceField.metric.name == "2m Temperature").first()
    rain_sourcefield = SourceField.query(SourceField.source_id == , SourceField.metric.name == "Raining").first()
    snow_sourcefield = SourceField.query(SourceField.source_id == , SourceField.metric.name == "Snowing").first()
    wind_sourcefield = SourceField.query(SourceField.source_id == , SourceField.metric.name == "Wind").first()

    # TODO: This should be done relative to the location's local TZ
    now = datetime.utcnow()
    days = int(request.args.get('days', 7))

    if days < 1:
        days = 1
    elif days > 7:
        days = 7

    summarizations = []

    for d in range(days):
        summary = SummarizedData()

        loc_data = LocationData.query.filter(
            LocationData.location_id == location.id,
            LocationData.valid_time >= now + timedelta(days=d),
            LocationData.valid_time < now + timedelta(days=d+1),
        ).all()

        model_loc_data = combine_models(loc_data)

        for data in model_loc_data:
            for data_point in data.values:
                if data_point['src_field_id'] == temp_sourcefield.id:
                    if data_point['value'] < summary.low.temperature:
                        summary.low = TemperatureEvent(time=data.valid_time, temperature=data_point['value'])
                    elif data_point['value'] > summary.high.temperature:
                        summary.high = TemperatureEvent(time=data.valid_time, temperature=data_point['value'])

        # TODO: extract intensity ('light', 'heavy') for rain/snow
        rain_periods = [PrecipEvent(start, end, 'rain') for start, end, _ in cluster(model_loc_data, rain_sourcefield.id)]
        snow_periods = [PrecipEvent(start, end, 'snow') for start, end, _ in cluster(model_loc_data, snow_sourcefield.id)]

        summary.precip_events = sorted(rain_periods + snow_periods, key=lambda event: event.start)

        summary.wind_events = [WindEvent(start, end, max(values)) for start, end, values in cluster(model_loc_data, wind_sourcefield.id, lambda v: v >= 30)]

        summarizations.append(summary.dict())

    return jsonify(summarizations)
