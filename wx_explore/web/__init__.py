#!/usr/bin/env python3
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_bootstrap import Bootstrap

from wx_explore.web import config

app = Flask(__name__)
app.config.from_object(config.DevConfig)

Bootstrap(app)

db = SQLAlchemy(app)

from wx_explore.web.data.controller import api

app.register_blueprint(api)

db.create_all()