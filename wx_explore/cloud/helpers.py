from sqlalchemy import create_engine
import base64
import boto3
import os


def s3_client(args):
    if 's3_creds' in args:
        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            aws_access_key_id=args['s3_creds']['access_key'],
            aws_secret_access_key=args['s3_creds']['secret_key'],
            endpoint_url=args['s3_creds']['endpoint'],
        )
    else:
        s3 = boto3.client('s3')

    return s3


def db_engine(args):
    if '__bx_creds' in args:
        conn_creds = args['__bx_creds']['databases-for-postgresql']['connection']['postgres']
        with open('/tmp/db.crt', 'wb') as crt:
            crt.write(base64.b64decode(conn_creds['certificate']['certificate_base64']))
        return create_engine(conn_creds['composed'][0] + "&sslrootcert=/tmp/db.crt")
    # TODO: AWS IAM-based DB credential retrieving
    elif 'CONFIG' in os.environ:
        from wx_explore.web import config
        cfg = config.getattr(os.environ['CONFIG'])
        return create_engine(cfg.SQLALCHEMY_DATABASE_URI)
    else:
        print("Warning: defaulting to sqlite://db.db")
        return create_engine("sqlite://db.db")