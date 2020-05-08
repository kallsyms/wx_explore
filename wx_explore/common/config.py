import os


class Config():
    SECRET_KEY = os.environ.get('SECRET_KEY', os.urandom(32))

    POSTGRES_USER = os.environ['POSTGRES_USER']
    POSTGRES_PASS = os.environ['POSTGRES_PASS']
    POSTGRES_HOST = os.environ['POSTGRES_HOST']
    POSTGRES_PORT = int(os.environ.get('POSTGRES_PORT', 5432))
    POSTGRES_DB   = os.environ['POSTGRES_DB']
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    INGEST_S3_ACCESS_KEY = os.environ['INGEST_S3_ACCESS_KEY']
    INGEST_S3_SECRET_KEY = os.environ['INGEST_S3_SECRET_KEY']
    INGEST_S3_REGION     = os.environ['INGEST_S3_REGION']
    INGEST_S3_BUCKET     = os.environ['INGEST_S3_BUCKET']
    INGEST_S3_ENDPOINT   = os.environ.get('INGEST_S3_ENDPOINT')

    SENTRY_ENDPOINT = os.environ.get('SENTRY_ENDPOINT')

    TRACE_EXPORTER = os.environ.get('TRACE_EXPORTER')

    JAEGER_HOST = os.environ.get('JAEGER_HOST', 'jaeger')
    HONEYCOMB_API_KEY = os.environ.get('HONEYCOMB_API_KEY')
    HONEYCOMB_DATASET = os.environ.get('HONEYCOMB_DATASET')


Config.SQLALCHEMY_DATABASE_URI = f"postgres://{Config.POSTGRES_USER}:{Config.POSTGRES_PASS}@{Config.POSTGRES_HOST}:{Config.POSTGRES_PORT}/{Config.POSTGRES_DB}"
