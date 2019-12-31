from flask import Flask
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy

from wx_explore.common.config import Config

app = Flask(__name__)
app.config.from_object(Config)
CORS(app)

from wx_explore.common.models import Base
db = SQLAlchemy(app, model_class=Base)

from wx_explore.web.api import api
app.register_blueprint(api)

db.create_all()

@app.before_first_request
def preload():
    from wx_explore.common.location import preload_coordinate_lookup_meta
    preload_coordinate_lookup_meta()
