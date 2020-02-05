from flask import Flask
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy

from wx_explore.common.config import Config

app = Flask(__name__)
app.config.from_object(Config)
CORS(app)

from wx_explore.common.models import Base
db = SQLAlchemy(app, model_class=Base)

db.create_all()
