#!/usr/bin/env python3
from flask import Blueprint, abort, jsonify, request
from datetime import datetime, timedelta

from wx_explore.common.utils import datetime2unix
from wx_explore.web.data.models import (
    Source,
    Location,
    Metric,
    LocationData,
)
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
    search += '%'

    query = Location.query.filter(Location.name.ilike(search)).limit(10)

    return jsonify([l.serialize() for l in query.all()])


@api.route('/location/by_coords')
def get_location_from_coords():
    """
    Get the nearest location from a given lat, lon.
    :return: The location.
    """

    lat = float(request.args['lat'])
    lon = float(request.args['lon'])

    # TODO: may need to add distance limit if perf drops
    location = Location.query.order_by(Location.location.distance_centroid('POINT({} {})'.format(lon, lat))).first()

    return jsonify(location.serialize())


@api.route('/location/<int:loc_id>')
def get_location(loc_id):
    """
    Get information about a specific location.
    :param loc_id: The ID of the location to get information about.
    :return: The location.
    """
    location = Location.query.get_or_404(loc_id)
    return jsonify(location.serialize())


@api.route('/location/<int:loc_id>/wx')
def wx_for_location(loc_id):
    """
    Gets the weather for a specific location, optionally limiting by metric and time.
    :param loc_id: The ID of the location to get weather for.
    :return: An object mapping UNIX timestamp to a list of metrics representing the weather for the given location
    at that time.
    """
    location = Location.query.get_or_404(loc_id)

    requested_metrics = request.args.get('metrics')

    if requested_metrics:
        metrics = [Metric.query.get(i) for i in requested_metrics.split(',')]
    else:
        metrics = Metric.query.all()

    now = datetime.utcnow()
    start = request.args.get('start')
    end = request.args.get('end')

    if start is None:
        start = now - timedelta(hours=1)
    else:
        start = datetime.utcfromtimestamp(int(start))

        if not app.debug:
            if start < now - timedelta(days=1):
                start = now - timedelta(days=1)

    if end is None:
        end = now + timedelta(hours=12)
    else:
        end = datetime.utcfromtimestamp(int(end))

        if not app.debug:
            if end > now + timedelta(days=7):
                end = now + timedelta(days=7)

    # Get all data points for the location and times specified.
    # This is a dictionary mapping str(valid_time) -> list of metric values
    loc_data = LocationData.query.filter_by(location_id=location.id).first().values

    # Turn the str(int) keys into datetime keys, filtering out times we don't want
    loc_data = {
        datetime.utcfromtimestamp(int(valid_time)): vals
        for valid_time, vals in loc_data.items() if start <= datetime.utcfromtimestamp(int(valid_time)) < end
    }

    # wx['data'] is a dict of unix_time->metrics, where each metric may have multiple values from different sources
    wx = {
        'ordered_times': sorted(datetime2unix(valid_time) for valid_time in loc_data),
        'data': {datetime2unix(valid_time): [] for valid_time in loc_data},
    }

    requested_source_field_ids = set()
    for metric in metrics:
        for sf in metric.fields:
            requested_source_field_ids.add(sf.id)

    for valid_time, values in loc_data.items():
        for data_point in values:
            if data_point['src_field_id'] in requested_source_field_ids:
                wx['data'][datetime2unix(valid_time)].append(data_point)

    return jsonify(wx)
