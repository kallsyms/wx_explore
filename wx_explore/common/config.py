import os


class Config():
    SECRET_KEY = os.environ.get('SECRET_KEY', os.urandom(32))

    POSTGRES_USER = "bismuth"
    POSTGRES_PASS = os.environ['BISMUTH_AUTH']
    POSTGRES_HOST = "169.254.169.254"
    POSTGRES_PORT = 5432
    POSTGRES_DB   = "bismuth"
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    INGEST_MONGO_SERVER_URI = "mongodb://cloud.vortexweather.tech:27017/"
    INGEST_MONGO_DATABASE   = "wx"
    INGEST_MONGO_COLLECTION = "wx"


Config.SQLALCHEMY_DATABASE_URI = f"postgresql://{Config.POSTGRES_USER}:{Config.POSTGRES_PASS}@{Config.POSTGRES_HOST}:{Config.POSTGRES_PORT}/{Config.POSTGRES_DB}"
