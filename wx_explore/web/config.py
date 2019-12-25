import os

class Config():
    DEBUG = os.environ.get('DEBUG', False)
    SECRET_KEY = os.environ.get('SECRET_KEY', os.urandom(32))

    POSTGRES_USER = os.environ['POSTGRES_USER']
    POSTGRES_PASS = os.environ['POSTGRES_PASS']
    POSTGRES_HOST = os.environ['POSTGRES_HOST']
    POSTGRES_DB   = os.environ['POSTGRES_DB']
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    INGEST_S3_ACCESS_KEY = os.environ['INGEST_S3_ACCESS_KEY']
    INGEST_S3_SECRET_KEY = os.environ['INGEST_S3_SECRET_KEY']
    INGEST_S3_REGION     = os.environ['INGEST_S3_REGION']
    INGEST_S3_BUCKET     = os.environ['INGEST_S3_BUCKET']
    INGEST_S3_ENDPOINT   = os.environ.get('INGEST_S3_ENDPOINT')
