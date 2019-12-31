import array
import boto3
import concurrent.futures
import datetime
import hashlib
import hmac
import requests
import urllib.parse

from wx_explore.common.config import Config
from wx_explore.common.location import get_xy_for_coord, proj_shape
from wx_explore.common.models import (
    SourceField,
    FileBandMeta,
)


def get_s3_bucket():
    return boto3.resource(
        's3',
        aws_access_key_id=Config.INGEST_S3_ACCESS_KEY,
        aws_secret_access_key=Config.INGEST_S3_SECRET_KEY,
        region_name=Config.INGEST_S3_REGION,
        endpoint_url=Config.INGEST_S3_ENDPOINT,
    ).Bucket(Config.INGEST_S3_BUCKET)


# Most of the below is copied from https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html

def sign(key, msg):
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()


def get_signature_key(key, dateStamp, regionName, serviceName):
    kDate = sign(('AWS4' + key).encode('utf-8'), dateStamp)
    kRegion = sign(kDate, regionName)
    kService = sign(kRegion, serviceName)
    kSigning = sign(kService, 'aws4_request')
    return kSigning


def s3_request(path, headers):
    access_key = Config.INGEST_S3_ACCESS_KEY
    secret_key = Config.INGEST_S3_SECRET_KEY
    region = Config.INGEST_S3_REGION
    endpoint = Config.INGEST_S3_ENDPOINT
    bucket = Config.INGEST_S3_BUCKET

    canonical_uri = '/' + bucket + '/' + path

    t = datetime.datetime.utcnow()
    amzdate = t.strftime('%Y%m%dT%H%M%SZ')
    datestamp = t.strftime('%Y%m%d')

    canonical_querystring = ''
    host = urllib.parse.urlparse(endpoint).netloc
    canonical_headers = 'host:' + host + '\n' + 'x-amz-date:' + amzdate + '\n'
    signed_headers = 'host;x-amz-date'
    payload_hash = hashlib.sha256(('').encode('utf-8')).hexdigest()

    canonical_request = 'GET\n' + canonical_uri + '\n' + canonical_querystring + '\n' + canonical_headers + '\n' + signed_headers + '\n' + payload_hash

    algorithm = 'AWS4-HMAC-SHA256'
    credential_scope = datestamp + '/' + region + '/' + 's3' + '/' + 'aws4_request'
    string_to_sign = algorithm + '\n' +  amzdate + '\n' +  credential_scope + '\n' +  hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()

    signing_key = get_signature_key(secret_key, datestamp, region, 's3')

    signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()

    authorization_header = algorithm + ' ' + 'Credential=' + access_key + '/' + credential_scope + ', ' +  'SignedHeaders=' + signed_headers + ', ' + 'Signature=' + signature

    amz_headers = {'x-amz-date': amzdate, 'Authorization': authorization_header}

    return requests.get(endpoint + canonical_uri, headers={**headers, **amz_headers})


def load_file_chunk(fm, coords):
    x, y = coords

    n_x = proj_shape(fm.projection)[1]
    loc_chunks = (y * n_x) + x

    start = loc_chunks * fm.loc_size
    end = (loc_chunks + 1) * fm.loc_size

    return s3_request(fm.file_name, {'Range': f'bytes={start}-{end-1}'}).content


def load_data_points(coords, start, end, source_fields=None):
    if source_fields is None:
        source_fields = SourceField.query.all()

    valid_source_fields = []
    locs = {}
    for sf in source_fields:
        if sf.projection_id in locs and locs[sf.projection_id] is None:
            continue

        if sf.projection_id not in locs:
            loc = get_xy_for_coord(sf.projection, coords)
            if loc is None:
                continue

            locs[sf.projection_id] = loc

        valid_source_fields.append(sf)

    fbms = FileBandMeta.query.filter(
        FileBandMeta.source_field_id.in_([sf.id for sf in valid_source_fields]),
        FileBandMeta.valid_time >= start,
        FileBandMeta.valid_time < end,
    ).all()

    # Gather all files we need data from
    file_metas = set(fbm.file_meta for fbm in fbms)

    file_contents = {}

    # Read them in
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(load_file_chunk, fm, locs[fm.projection_id]): fm for fm in file_metas}
        for future in concurrent.futures.as_completed(futures):
            fm = futures[future]
            file_contents[fm.file_name] = future.result()

    # filebandmeta -> values
    data_points = {}

    for fbm in fbms:
        raw = file_contents[fbm.file_name][fbm.offset:fbm.offset+(4*fbm.vals_per_loc)]
        data_values = array.array("f", raw).tolist()
        data_points[fbm] = data_values

    return data_points
