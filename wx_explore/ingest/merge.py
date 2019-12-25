from datetime import datetime, timedelta
import collections
import hashlib
import logging
import tempfile

from wx_explore.common.location import proj_shape
from wx_explore.common.models import (
    FileMeta,
    FileBandMeta,
)
from wx_explore.common.storage import get_s3_bucket
from wx_explore.web import db

logger = logging.getLogger(__name__)


def merge():
    """
    Merge all small files into larger files to reduce the number of S3 requests each query needs to do.
    """
    # 1024 is somewhat arbitrary.
    # This limit prevents all data points from being merged into a single large file which would
    #  1) never get cleaned up/removed
    #  2) be super inefficient to add to since it would be copying around 10s to 100s of GB
    max_size = 1024
    # min_age prevents brand new files that may still be uploading from being merged.
    # This also has the side-effect of batching together small files that then merge into larger files
    # that then merge into larger files, etc. since a newly merged file wont be eligible in the next
    # merge cycle.
    min_age = datetime.utcnow() - timedelta(hours=1)

    small_files = FileMeta.query.filter(FileMeta.loc_size < max_size, FileMeta.ctime < min_age).all()

    proj_files = collections.defaultdict(list)
    for f in small_files:
        proj_files[f.projection].append(f)

    s3 = get_s3_bucket()

    for proj, files in proj_files.items():
        if len(files) < 2:
            continue

        logger.info("Merging %s", ','.join(f.file_name for f in files))

        n_y, n_x = proj_shape(proj)
        merged_loc_size = sum(f.loc_size for f in files)
        s3_file_name = hashlib.md5(('-'.join(f.file_name for f in files)).encode('utf-8')).hexdigest()

        with tempfile.TemporaryFile() as merged:
            # Get file contents every few rows. Good middle between entire files (too much data to store in memory)
            # and each (x,y) (too many requests to S3).
            buffer_lines = 50

            for y in range(n_y):
                if y % buffer_lines == 0:
                    file_contents = {}
                    # TODO: parallelize
                    for f in files:
                        obj = s3.Object(f.file_name)
                        start = (y * n_x) * f.loc_size
                        end = ((y + buffer_lines) * n_x) * f.loc_size
                        end = min(end, n_y * n_x * f.loc_size)
                        file_contents[f] = obj.get(Range=f'bytes={start}-{end-1}')['Body'].read()

                for x in range(n_x):
                    start = ((y % buffer_lines) * n_x) + x
                    for f in files:
                        merged.write(file_contents[f][start*f.loc_size:(start+1)*f.loc_size])

            merged.seek(0)
            s3.upload_fileobj(merged, s3_file_name)
            logger.info("Created merged S3 file %s", s3_file_name)

        merged_meta = FileMeta(
            file_name=s3_file_name,
            projection_id=proj.id,
            loc_size=merged_loc_size,
        )
        db.session.add(merged_meta)
        db.session.commit()

        offset = 0

        for f in files:
            for band in f.bands:
                band.file_name = merged_meta.file_name
                band.offset += offset
            offset += f.loc_size

        db.session.commit()
        logger.info("Updated file band meta")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    merge()
