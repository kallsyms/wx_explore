import boto3
from wx_explore.web import app


def get_s3_bucket(session=boto3):
    return session.resource(
        's3',
        aws_access_key_id=app.config['INGEST_S3_ACCESS_KEY'],
        aws_secret_access_key=app.config['INGEST_S3_SECRET_KEY'],
        region_name=app.config['INGEST_S3_REGION'],
        endpoint_url=app.config['INGEST_S3_ENDPOINT'],
    ).Bucket(app.config['INGEST_S3_BUCKET'])
