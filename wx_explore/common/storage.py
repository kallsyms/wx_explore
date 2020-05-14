from typing import Tuple, Optional, Iterable, Dict, List
from aws_requests_auth.aws_auth import AWSRequestsAuth

import array
import boto3
import concurrent.futures
import datetime
import logging
import requests
import urllib.parse

from wx_explore.common import tracing
from wx_explore.common.config import Config
from wx_explore.common.location import get_xy_for_coord
from wx_explore.common.models import (
    SourceField,
    FileBandMeta,
    DataPointSet,
)


logger = logging.getLogger(__name__)


def get_s3_bucket(session=boto3):
    return session.resource(
        's3',
        aws_access_key_id=Config.INGEST_S3_ACCESS_KEY,
        aws_secret_access_key=Config.INGEST_S3_SECRET_KEY,
        region_name=Config.INGEST_S3_REGION,
        endpoint_url=Config.INGEST_S3_ENDPOINT,
    ).Bucket(Config.INGEST_S3_BUCKET)


S3_AUTH = AWSRequestsAuth(
    aws_access_key=Config.INGEST_S3_ACCESS_KEY,
    aws_secret_access_key=Config.INGEST_S3_SECRET_KEY,
    aws_host=urllib.parse.urlsplit(Config.INGEST_S3_ENDPOINT).netloc,
    aws_region=Config.INGEST_S3_REGION,
    aws_service='s3',
)


def s3_path(path):
    if Config.INGEST_S3_ENDPOINT is None:
        # Make the proper S3 endpoint.
        # Since this is basically in a hot path on multiple threads, avoid boto3 calls
        # because those apparently dynamically load, parse, and eval JSON off of disk
        # which is _slooowwww_
        return f"https://{Config.INGEST_S3_BUCKET}.s3.{Config.INGEST_S3_REGION}.amazonaws.com/{path}"

    # Manual endpoint, assume path style
    return f"{Config.INGEST_S3_ENDPOINT}/{Config.INGEST_S3_BUCKET}/{path}"


def s3_get(path, **kwargs):
    for _ in range(3):
        try:
            resp = requests.get(s3_path(path), auth=S3_AUTH, **kwargs)
            if resp.ok:
                return resp
        except Exception as e:
            logger.warning("Exception getting from S3: %s", e)
            continue
        logger.warning("Unexpected response getting from S3: %s", resp)

    raise Exception(f"Unable to get {path} from S3 - maximum retries exceeded")


def s3_put(path, data, **kwargs):
    for _ in range(3):
        try:
            resp = requests.put(s3_path(path), data=data, auth=S3_AUTH, **kwargs)
            if resp.ok:
                return resp
        except Exception as e:
            logger.warning("Exception uploading to S3: %s", e)
            continue
        logger.warning("Unexpected response uploading to S3: %s", resp)

    raise Exception(f"Unable to upload {path} to S3 - maximum retries exceeded")


def load_file_chunk(fm, coords):
    x, y = coords

    start = x * fm.loc_size
    end = (x + 1) * fm.loc_size

    return s3_get(f"{y}/{fm.file_name}", headers={'Range': f'bytes={start}-{end-1}'}).content


def load_data_points(
        coords: Tuple[float, float],
        start: datetime.datetime,
        end: datetime.datetime,
        source_fields: Optional[Iterable[SourceField]] = None
) -> List[DataPointSet]:

    if source_fields is None:
        source_fields = SourceField.query.all()

    # Determine all valid source fields (fields in source_fields which cover the given coords),
    # and the x,y for projection used in any valid source field.
    valid_source_fields = []
    locs: Dict[int, Tuple[float, float]] = {}
    for sf in source_fields:
        if sf.projection_id in locs and locs[sf.projection_id] is None:
            continue

        if sf.projection_id not in locs:
            with tracing.start_span("get_xy_for_coord") as span:
                span.set_attribute("projection_id", sf.projection_id)
                loc = get_xy_for_coord(sf.projection, coords)

            # Skip if given projection does not cover coords
            if loc is None:
                continue

            locs[sf.projection_id] = loc

        valid_source_fields.append(sf)

    with tracing.start_span("load file band metas") as span:
        fbms: List[FileBandMeta] = FileBandMeta.query.filter(
            FileBandMeta.source_field_id.in_([sf.id for sf in valid_source_fields]),
            FileBandMeta.valid_time >= start,
            FileBandMeta.valid_time < end,
        ).all()

    # Gather all files we need data from
    file_metas = set(fbm.file_meta for fbm in fbms)

    file_contents = {}

    # Read them in (in parallel)
    # TODO: use asyncio here instead once everything else is ported?
    with tracing.start_span("load file chunks") as span:
        span.set_attribute("num_files", len(file_metas))
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(load_file_chunk, fm, locs[fm.projection_id]): fm for fm in file_metas}
            for future in concurrent.futures.as_completed(futures):
                fm = futures[future]
                file_contents[fm.file_name] = future.result()

    # filebandmeta -> values
    data_points = []
    for fbm in fbms:
        raw = file_contents[fbm.file_name][fbm.offset:fbm.offset+(4*fbm.vals_per_loc)]
        data_values: List[float] = array.array("f", raw).tolist()
        data_point = DataPointSet(
            values=data_values,
            metric_id=fbm.source_field.metric.id,
            valid_time=fbm.valid_time,
            source_field_id=fbm.source_field_id,
            run_time=fbm.run_time,
        )

        data_points.append(data_point)

    return data_points
