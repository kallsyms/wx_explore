from datetime import datetime, timedelta
from flask import Blueprint, abort, jsonify, request
from sqlalchemy import and_, or_

from wx_explore.common.models import (
    Source,
    Location,
    Metric,
    PointData,
)
from wx_explore.common.location import get_xy_for_coord
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
    search += '%'

    query = Location.query.filter(Location.name.ilike(search)).limit(10)

    return jsonify([l.serialize() for l in query.all()])


@api.route('/location/<float:lat>/<float:lon>/wx')
def wx_for_location(lat, lon):
    """
    Gets the weather for a specific location, optionally limiting by metric and time.
    :param lat: The latitude of the location to get weather for.
    :param lon: The longitude of the location to get weather for.
    :return: An object mapping UNIX timestamp to a list of metrics representing the weather for the given location
    at that time.
    """
    if lat > 90 or lat < -90 or lon > 180 or lon < 180:
        abort(400)

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

    requested_source_field_ids = set()
    for metric in metrics:
        for sf in metric.fields:
            requested_source_field_ids.add(sf.id)

    locs = {}
    for sfid in requested_source_field_ids:
        locs[sfid] = get_xy_for_coord(sfid, (lat,lon))

    predicates = []
    for sfid, (proj_id, x, y) in locs.items():
        predicates.append(and_(
            PointData.projection_id == proj_id,
            PointData.x == x,
            PointData.y == y,
        ))

    # Get all data points for the location and times specified.
    # This is a dictionary mapping str(valid_time) -> list of metric values
    loc_data = PointData.query.filter(
        or_(*predicates),
        PointData.valid_time >= start,
        PointData.valid_time < end,
    ).all()

    # wx['data'] is a dict of unix_time->metrics, where each metric may have multiple values from different sources
    wx = {
        'ordered_times': sorted(datetime2unix(data.valid_time) for data in loc_data),
        'data': {datetime2unix(data.valid_time): [] for data in loc_data},
    }

    for data in loc_data:
        for data_point in data.values:
            if data_point['src_field_id'] in requested_source_field_ids:
                wx['data'][datetime2unix(data.valid_time)].append(data_point)

    return jsonify(wx)
