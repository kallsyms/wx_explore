from flask import Flask, render_template
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


@app.route('/raw/<string:zipcode>')
def raw_data(zipcode):
    return render_template('raw_data.html', zipcode=zipcode, datas=DataPoint.query.filter_by(zipcode=zipcode).all())


if __name__ == '__main__':
    app.run(port=5001)
