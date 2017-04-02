from flask import Flask, render_template, abort, jsonify, request
from flask.ext.sqlalchemy import SQLAlchemy
from flask_bootstrap import Bootstrap
from datetime import datetime, timedelta

import config


app = Flask(__name__)
app.config.from_object('config.DevConfig')

Bootstrap(app)

db = SQLAlchemy(app)

from models import *


@app.route('/sources')
def sources():
    return render_template('sources.html', sources=Source.query.all())


@app.route('/raw/<int:loc_id>')
def raw_data(loc_id):
    location = Location.query.get(loc_id)
    if location:
        return render_template('raw_data.html', location=location, datas=location.data_points)
    else:
        abort(404)


@app.route('/api/sources')
def get_sources():
    res = []

    for source in Source.query.all():
        j = source.serialize()
        j['fields'] = [f.serialize() for f in source.fields]
        res.append(j)

    return jsonify(res)


@app.route('/api/source/<int:src_id>')
def get_source(src_id):
    source = Source.query.get(src_id)

    if not source:
        abort(404)

    j = source.serialize()
    j['fields'] = [f.serialize() for f in source.fields]

    return jsonify(j)


@app.route('/api/metrics')
def get_metrics():
    return jsonify([m.serialize() for m in Metric.query.all()])


@app.route('/api/location/search')
def get_location_from_query():
    search = request.args.get('q')

    if search is None or len(search) < 3:
        abort(400)

    search = search.replace('_', '').replace('%', '')
    search += '%'

    query = Location.query.filter(Location.name.ilike(search)).limit(10)

    return jsonify([l.serialize() for l in query.all()])


@app.route('/api/location/by_coords')
def get_location_from_coords():
    lat = request.args.get('lat')
    lon = request.args.get('lon')

    if lat is None or lon is None:
        abort(400)

    try:
        lat = float(lat)
        lon = float(lon)
    except ValueError:
        abort(400)

    # TODO: may need to add distance limit if perf drops
    query = Location.query.order_by(Location.location.distance_centroid('POINT({} {})'.format(lon, lat)))

    return jsonify(query.first().serialize())


@app.route('/api/location/<int:loc_id>')
def get_location(loc_id):
    location = Location.query.get(loc_id)

    if location is None:
        abort(404)

    return jsonify(location.serialize())


@app.route('/api/location/<int:loc_id>/wx')
def wx_for_location(loc_id):
    location = Location.query.get(loc_id)

    if location is None:
        abort(404)

    requested_metrics = request.args.get('metrics')

    if requested_metrics:
        try:
            requested_metrics = requested_metrics.split(',')
        except AttributeError:
            abort(400)
        metrics = [Metric.query.get(i) for i in requested_metrics]
    else:
        metrics = Metric.query.all()

    now = datetime.utcnow()
    start = request.args.get('start')
    end = request.args.get('end')

    if start is None:
        start = now - timedelta(hours=1)
    else:
        try:
            start = datetime.utcfromtimestamp(int(start))
        except ValueError:
            abort(400)

        if start < now - timedelta(days=1):
            start = now - timedelta(days=1)

    if end is None:
        end = now + timedelta(hours=12)
    else:
        try:
            end = datetime.utcfromtimestamp(int(end))
        except ValueError:
            abort(400)

        if end > now + timedelta(days=7):
            end = now + timedelta(days=7)

    data_points = DataPoint.query.filter(DataPoint.location_id == location.id,
                                         DataPoint.time >= start,
                                         DataPoint.time <= end).all()

    times = sorted(set([d.time for d in data_points]))

    wx = {}

    for i, time in enumerate(times):
        wx[i] = {}
        wx[i]['time'] = time
        for m in metrics:
            wx[i][m.name] = []

    for d in data_points:
        wx[times.index(d.time)][d.src_field.metric.name].append({"value": d.value, "src_id": d.src_field_id})

    return jsonify(wx)


if __name__ == '__main__':
    app.run(port=5001)
