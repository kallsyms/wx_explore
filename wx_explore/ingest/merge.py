import collections
import concurrent.futures
import hashlib
import logging
import numpy
import tempfile

from wx_explore.common.models import (
    FileMeta,
    FileBandMeta,
)
from wx_explore.common.storage import s3_get, session_allocator, get_s3_bucket
from wx_explore.web.core import db

logger = logging.getLogger(__name__)


def create_merged_stripe(files, used_idxs, s3_file_name, n_x, y):
    with tempfile.TemporaryFile() as merged:
        contents = []
        for f in files:
            datas = numpy.frombuffer(
                s3_get(f"{y}/{f.file_name}").content,
                dtype=numpy.float32,
            ).reshape((n_x, f.loc_size//4))

            contents.append(datas[:, used_idxs[f]])

        merged.write(numpy.concatenate(contents, axis=1).tobytes())

        merged.seek(0)

        with session_allocator.get_session() as s:
            s3 = get_s3_bucket(s)
            s3.upload_fileobj(merged, f"{y}/{s3_file_name}")


def merge():
    """
    Merge all small files into larger files to reduce the number of S3 requests each query needs to do.
    """
    small_files = FileMeta.query.filter(
        FileMeta.file_name.in_(FileBandMeta.query.with_entities(FileBandMeta.file_name)),
    ).all()

    proj_files = collections.defaultdict(list)
    for f in small_files:
        proj_files[f.projection].append(f)

    for proj, files in proj_files.items():
        n_y, n_x = proj.shape()

        if len(files) < 2:
            continue

        logger.info("Merging %s", ','.join(f.file_name for f in files))

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

        # If we fail to create any merged band, don't commit the changes to
        # band offset/file name, but _do_ commit the FileMeta to the DB.
        # This way the normal cleaning process will remove any orphaned bands.
        commit_merged = True

        # max workers = 10 to limit mem utilization
        # Approximate worst case, we'll have
        # (5 sources * 70 runs * 2000 units wide * 20 metrics/unit * 4 bytes per metric) per row
        # or ~50MB/row in memory.
        # 10 rows keeps us well under 1GB which is what this should be provisioned for.
        with concurrent.futures.ThreadPoolExecutor(10) as executor:
            session_allocator.alloc_sessions(10)
            futures = concurrent.futures.wait([
                executor.submit(create_merged_stripe, files, used_idxs, s3_file_name, n_x, y)
                for y in range(n_y)
            ])
            for fut in futures.done:
                if fut.exception() is not None:
                    logger.error("Exception merging: %s", fut.exception())
                    commit_merged = False

        if commit_merged:
            for band, offset in new_offsets.items():
                band.offset = offset
                band.file_name = merged_meta.file_name

            logger.info("Updated file band meta")

        db.session.commit()



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    merge()
