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
from wx_explore.common.storage import get_s3_bucket, s3_request
from wx_explore.web import db

logger = logging.getLogger(__name__)


def merge(max_size=2048):
    """
    Merge all small files into larger files to reduce the number of S3 requests each query needs to do.
    """
    # max_size=2048 is somewhat arbitrary.
    # This limit prevents all data points from being merged into a single large file which would
    #  1) never get cleaned up/removed
    #  2) be super inefficient to add to since it would be copying around 10s to 100s of GB

    # min_age prevents brand new files that may still be uploading from being merged.
    # This also has the side-effect of batching together small files that then merge into larger files
    # that then merge into larger files, etc. since a newly merged file wont be eligible in the next
    # merge cycle.
    min_age = datetime.utcnow() - timedelta(hours=1)

    small_files = FileMeta.query.filter(
        FileMeta.loc_size < (max_size - 4),
        FileMeta.ctime < min_age,
        FileMeta.file_name.in_(FileBandMeta.query.with_entities(FileBandMeta.file_name)),
    ).all()

    proj_files = collections.defaultdict(list)
    for f in small_files:
        proj_files[f.projection].append(f)

    s3 = get_s3_bucket()

    for proj, all_files in proj_files.items():
        # group all_files into a list of lists (of files),
        # each list of files not having a total loc_size > max_size
        merge_groups = [[]]
        for f in all_files:
            if sum(fl.loc_size for fl in merge_groups[-1]) > max_size:
                merge_groups.append([])
            merge_groups[-1].append(f)

        n_y, n_x = proj_shape(proj)

        for files in merge_groups:
            if len(files) < 2:
                continue

            logger.info("Merging %s", ','.join(f.file_name for f in files))

            merged_loc_size = sum(f.loc_size for f in files)
            s3_file_name = hashlib.md5(('-'.join(f.file_name for f in files)).encode('utf-8')).hexdigest()
            s3_file_name = s3_file_name[:2] + '/' + s3_file_name

            requests = {f: s3_request(f.file_name, stream=True) for f in files}
            with tempfile.TemporaryFile() as merged:
                for y in range(n_y):
                    for x in range(n_x):
                        for f, req in requests.items():
                            merged.write(req.raw.read(f.loc_size))

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
