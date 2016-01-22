from flask import Flask, render_template, abort
from flask.ext.sqlalchemy import SQLAlchemy
from flask_bootstrap import Bootstrap
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


if __name__ == '__main__':
    app.run(port=5001)
