from flask import Flask, render_template, abort
from flask.ext.sqlalchemy import SQLAlchemy
from flask_bootstrap import Bootstrap
from datetime import datetime
import config


app = Flask(__name__)
app.config.from_object('config.DevConfig')

Bootstrap(app)

db = SQLAlchemy(app)

from models import *


@app.route('/sources')
def sources():
    return render_template('sources.html', sources=Source.query.all())


@app.route('/raw/<string:loc_id>')
def raw_data(loc_id):
    location = Location.query.get(loc_id)
    if location:
        return render_template('raw_data.html', location=location, datas=location.data_points)
    else:
        return abort(404)


@app.route('/location/<string:loc_id>')
def wx_for_location(loc_id):
    location = Location.query.get(loc_id)
    if location:
        metrics = Metric.query.all()

        wx = {}
        data_points = DataPoint.query\
            .filter(DataPoint.location_id == location.id,
                    DataPoint.time >= datetime.utcnow())\
            .all()
        times = sorted(set([d.time for d in data_points]))

        for i, time in enumerate(times):
            wx[i] = {}
            wx[i]['time'] = time
            for m in metrics:
                wx[i][m.name] = []

        for d in data_points:
            wx[times.index(d.time)][d.src_field.type.name].append(d.value)

        return render_template('loc_wx.html', location=location, wx=wx)
    else:
        return abort(404)


if __name__ == '__main__':
    app.run(port=5001)
