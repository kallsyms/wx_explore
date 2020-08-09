from aws_requests_auth.aws_auth import AWSRequestsAuth
from functools import partial
from math import ceil
from typing import List, Dict, Tuple

import array
import boto3
import collections
import concurrent.futures
import datetime
import hashlib
import logging
import numpy
import os
import random
import requests
import urllib.parse

from . import DataProvider
from wx_explore.common import tracing
from wx_explore.common.location import clear_proj_cache
from wx_explore.common.models import (
    Projection,
    SourceField,
    FileMeta,
    FileBandMeta,
    DataPointSet,
)
from wx_explore.common.utils import chunk
from wx_explore.web.core import db


class S3Backend(DataProvider):
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

    def put_fields(
            self,
            proj: Projection,
            fields: Dict[Tuple[int, datetime.datetime, datetime.datetime], List[numpy.array]]
    ):
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

    def clean(self, _oldest_time: datetime.datetime):
        files = FileMeta.query.filter(
            FileMeta.file_name.notin_(FileBandMeta.query.with_entities(FileBandMeta.file_name)),

            FileMeta.ctime <= datetime.datetime.utcnow() - datetime.timedelta(hours=1),  # make sure we don't delete files being populated right now
            # XXX: I don't think the above ctime check is actually necessary since all filemeta and filebandmeta
            # creation happens in one atomic commit.
        ).all()

        s3 = self._get_s3_bucket()

        for f in files:
            self.logger.info("Removing unused file group %s", f.file_name)
            for ys in chunk(range(f.projection.n_y), 1000):
                s3.delete_objects(Delete={'Objects': [{'Key': f"{y}/{f.file_name}"} for y in ys]})
            db.session.delete(f)
            db.session.commit()

        # Now that we've removed everything we know we can, look for any files in S3
        # which aren't tracked by a FileMeta. In theory this can only happen with
        # bad code, but is worth checking for to prevent S3 usage from growing
        # unbounded.
        self.logger.info("Finding orphaned files to remove...")
        known_fns = set(fm.file_name for fm in FileMeta.query.all())
        to_del = []

        for obj in s3.objects.all():
            # Ignore things that are new
            if obj.last_modified >= datetime.datetime.now(obj.last_modified.tzinfo) - datetime.timedelta(hours=3):
                continue

            if os.path.basename(obj.key) not in known_fns:
                to_del.append(obj.key)

        self.logger.info("Removing %d orphaned files", len(to_del))
        for grp in chunk(to_del, 1000):
            s3.delete_objects(Delete={'Objects': [{'Key': key} for key in grp]})

    ###
    # Merging
    ###

    def _load_stripe(self, used_idxs, y, n_x, f):
        stripe_req = self._s3_get(f"{y}/{f.file_name}")

        if len(stripe_req.content) != n_x * f.loc_size:
            raise ValueError(f"Invalid file size in {y}/{f.file_name}. Expected {n_x*f.loc_size}, got {len(stripe_req.content)}")

        datas = numpy.frombuffer(stripe_req.content, dtype=numpy.float32).reshape((n_x, f.loc_size//4))
        return datas[:, used_idxs[f]]

    def _create_merged_stripe(self, files, used_idxs, s3_file_name, n_x, y, trace_span):
        with tracing.start_span('parallel stripe loading', parent=trace_span):
            with concurrent.futures.ThreadPoolExecutor(10) as executor:
                contents = list(executor.map(partial(self._load_stripe, used_idxs, y, n_x), files))

        with tracing.start_span('merged stripe save', parent=trace_span):
            d = numpy.concatenate(contents, axis=1).tobytes()
            self._s3_put(f"{y}/{s3_file_name}", d)

    def merge(self):
        """
        Merge all (small) files into larger files to reduce the number of S3 requests each query needs to do.
        """
        all_files = FileMeta.query.filter(
            FileMeta.file_name.in_(FileBandMeta.query.filter(FileBandMeta.valid_time > datetime.datetime.utcnow()).with_entities(FileBandMeta.file_name)),
        ).order_by(
            FileMeta.loc_size.asc(),
        ).all()

        proj_files = collections.defaultdict(list)
        for f in all_files:
            proj_files[f.projection].append(f)

        # Pull from the projection with the most backlog first
        for proj, proj_files in sorted(proj_files.items(), key=lambda pair: len(pair[1]), reverse=True):
            # Don't waste time if we don't really have that many files
            if len(proj_files) < 8:
                continue

            # Merge in smaller batches (10 <= batch_size <= 50) to more quickly reduce S3 load per query.
            batch_size = min(ceil(len(proj_files) / 4), 50)
            if len(proj_files) < 40:
                batch_size = len(proj_files)

            for files in chunk(proj_files, batch_size):
                # This next part is all about figuring out what items are still used in
                # each file so that the merge process can effectively garbage collect
                # unused data.

                # Dict of FileMeta -> list of float32 item indexes still used by some band
                used_idxs = collections.defaultdict(list)

                offset = 0
                # Dict of FileBandMeta -> offset
                new_offsets = {}

                for f in files:
                    for band in f.bands:
                        # Don't bother merging old data. Prevents racing with the cleaner,
                        # and probably won't be queried anyways.
                        if band.valid_time < datetime.datetime.utcnow():
                            continue

                        new_offsets[band] = offset
                        offset += 4 * band.vals_per_loc

                        start_idx = band.offset // 4
                        used_idxs[f].extend(range(start_idx, start_idx + band.vals_per_loc))

                s3_file_name = hashlib.md5(('-'.join(f.file_name for f in files)).encode('utf-8')).hexdigest()

                merged_meta = FileMeta(
                    file_name=s3_file_name,
                    projection_id=proj.id,
                    loc_size=offset,
                )
                db.session.add(merged_meta)

                self.logger.info("Merging %s into %s", ','.join(f.file_name for f in files), s3_file_name)

                n_y, n_x = proj.shape()

                # If we fail to create any merged stripe, don't commit the changes to
                # band offset/file name, but _do_ commit the FileMeta to the DB.
                # This way the normal cleaning process will remove any orphaned bands.
                commit_merged = True

                # max workers = 10 to limit mem utilization
                # Approximate worst case, we'll have
                # (5 sources * 70 runs * 2000 units wide * 20 metrics/unit * 4 bytes per metric) per row
                # or ~50MB/row in memory.
                # 10 rows keeps us well under 1GB which is what this should be provisioned for.
                with tracing.start_span('parallel stripe creation') as span:
                    span.set_attribute("s3_file_name", s3_file_name)
                    span.set_attribute("num_files", len(files))

                    with concurrent.futures.ThreadPoolExecutor(10) as executor:
                        futures = concurrent.futures.wait([
                            executor.submit(self._create_merged_stripe, files, used_idxs, s3_file_name, n_x, y, span)
                            for y in range(n_y)
                        ])
                        for fut in futures.done:
                            if fut.exception() is not None:
                                self.logger.error("Exception merging: %s", fut.exception())
                                commit_merged = False

                    span.set_attribute("commit", commit_merged)

                if commit_merged:
                    for band, offset in new_offsets.items():
                        band.offset = offset
                        band.file_name = merged_meta.file_name

                    self.logger.info("Updated file band meta")

                db.session.commit()

            # We know we won't need this projection again, so clear it
            clear_proj_cache()
