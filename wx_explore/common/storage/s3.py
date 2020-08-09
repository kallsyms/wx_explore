from aws_requests_auth.aws_auth import AWSRequestsAuth
from typing import List, Dict, Tuple

import array
import boto3
import concurrent.futures
import datetime
import logging
import numpy
import random
import requests
import urllib.parse

from wx_explore.common import tracing
from wx_explore.common.models import (
    Projection,
    SourceField,
    FileMeta,
    FileBandMeta,
    DataPointSet,
)
from wx_explore.web.core import db


class S3Backend(object):
    logger: logging.Logger
    access_key: str
    secret_access_key: str
    region: str
    bucket: str
    endpiont: str

    def __init__(self, access_key, secret_access_key, region='us-east-1', bucket=None, endpoint=None):
        self.access_key = access_key
        self.secret_access_key = secret_access_key
        self.region = region
        self.bucket = bucket
        self.endpoint = endpoint

        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth = AWSRequestsAuth(
            aws_access_key=self.access_key,
            aws_secret_access_key=self.secret_access_key,
            aws_host=(
                urllib.parse.urlsplit(self.endpoint).netloc if self.endpoint
                else f"{self.bucket}.s3.{self.region}.amazonaws.com"),
            aws_region=region,
            aws_service='s3',
        )

    def _get_s3_bucket(self, session=boto3):
        return session.resource(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region,
            endpoint_url=self.endpoint,
        ).Bucket(self.bucket)

    def _s3_path(self, path):
        if self.endpoint is None:
            # Make the proper S3 endpoint.
            # Since this is basically in a hot path on multiple threads, avoid boto3 calls
            # because those apparently dynamically load, parse, and eval JSON off of disk
            # which is _slooowwww_
            return f"https://{self.bucket}.s3.{self.region}.amazonaws.com/{path}"

        # Manual endpoint, assume path style
        return f"{self.endpoint}/{self.bucket}/{path}"

    def _s3_get(self, path, **kwargs):
        for _ in range(3):
            try:
                resp = requests.get(self._s3_path(path), auth=self.auth, **kwargs)
                if resp.ok:
                    return resp
            except Exception as e:
                self.logger.warning("Exception getting from S3: %s", e)
                continue
            self.logger.warning("Unexpected response getting from S3: %s", resp)

        raise Exception(f"Unable to get {path} from S3 - maximum retries exceeded")

    def _s3_put(self, path, data, **kwargs):
        for _ in range(3):
            try:
                resp = requests.put(self._s3_path(path), data=data, auth=self.auth, **kwargs)
                if resp.ok:
                    return resp
            except Exception as e:
                self.logger.warning("Exception uploading to S3: %s", e)
                continue
            self.logger.warning("Unexpected response uploading to S3: %s", resp)

        raise Exception(f"Unable to upload {path} to S3 - maximum retries exceeded")

    def load_file_chunk(self, fm, coords):
        x, y = coords

        start = x * fm.loc_size
        end = (x + 1) * fm.loc_size

        return self._s3_get(f"{y}/{fm.file_name}", headers={'Range': f'bytes={start}-{end-1}'}).content

    def get_fields(
            self,
            proj_id: int,
            loc: Tuple[float, float],
            valid_source_fields: List[SourceField],
            start: datetime.datetime,
            end: datetime.datetime
    ) -> List[DataPointSet]:
        with tracing.start_span("load file band metas") as span:
            fbms: List[FileBandMeta] = FileBandMeta.query.filter(
                FileBandMeta.source_field.has(id=proj_id),
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
                futures = {executor.submit(self.load_file_chunk, fm, loc): fm for fm in file_metas}
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

    def put_fields(self, proj: Projection, fields: Dict[Tuple[int, datetime.datetime, datetime.datetime], List[numpy.array]]):
        # fields is map of (field_id, valid_time, run_time) -> [msg, ...]
        metas = []
        vals = []

        s3_file_name = ''.join(random.choices('0123456789abcdef', k=32))

        fm = FileMeta(
            file_name=s3_file_name,
            projection_id=proj.id,
        )
        db.session.add(fm)

        offset = 0
        for i, ((field_id, valid_time, run_time), msgs) in enumerate(fields.items()):
            metas.append(FileBandMeta(
                file_name=s3_file_name,
                source_field_id=field_id,
                valid_time=valid_time,
                run_time=run_time,
                offset=offset,
                vals_per_loc=len(msgs),
            ))

            for msg in msgs:
                vals.append(msg.astype(numpy.float32))
                offset += 4  # sizeof(float32)

        combined = numpy.stack(vals, axis=-1)
        fm.loc_size = offset

        self.logger.info("Creating file group %s", s3_file_name)

        with concurrent.futures.ThreadPoolExecutor(32) as executor:
            futures = concurrent.futures.wait([
                executor.submit(self._s3_put, f"{y}/{s3_file_name}", vals.tobytes())
                for y, vals in enumerate(combined)
            ])
            for fut in futures.done:
                if fut.exception() is not None:
                    self.logger.warning("Exception creating files: %s", fut.exception())

        db.session.add_all(metas)
        db.session.commit()
