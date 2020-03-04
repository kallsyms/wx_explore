#!/usr/bin/env python3
from datetime import datetime, timedelta
from sqlalchemy import func
import logging
import os

from wx_explore.common.logging import init_sentry
from wx_explore.common.models import (
    FileMeta,
    FileBandMeta,
)
from wx_explore.common.utils import chunk
from wx_explore.common.storage import get_s3_bucket
from wx_explore.web.core import db

logger = logging.getLogger(__name__)


def clean_old_datas():
    # Delete all band metadata that is too old
    oldest_time = datetime.utcnow() - timedelta(days=1)
    FileBandMeta.query.filter(FileBandMeta.valid_time < oldest_time).delete()
    db.session.commit()

    # For things >1day old and < now, only keep the most recent run per (sourcefield, valid_time)
    most_recent = FileBandMeta.query.with_entities(func.max(FileBandMeta.run_time), FileBandMeta.source_field_id, FileBandMeta.valid_time).filter(
        FileBandMeta.valid_time < datetime.utcnow() - timedelta(hours=1)
    ).group_by(FileBandMeta.source_field_id, FileBandMeta.valid_time).all()

    for (newest_run_time, sfid, valid_time) in most_recent:
        FileBandMeta.query.filter(
            FileBandMeta.source_field_id == sfid,
            FileBandMeta.valid_time == valid_time,
            FileBandMeta.run_time < newest_run_time,
        ).delete()

    db.session.commit()

    files = FileMeta.query.filter(
        FileMeta.file_name.notin_(FileBandMeta.query.with_entities(FileBandMeta.file_name)),

        FileMeta.ctime <= datetime.utcnow() - timedelta(hours=1),  # make sure we don't delete files being populated right now
        # XXX: I don't think the above ctime check is actually necessary since all filemeta and filebandmeta
        # creation happens in one atomic commit.
    ).all()

    s3 = get_s3_bucket()

    for f in files:
        logger.info("Removing unused file group %s", f.file_name)
        for ys in chunk(range(f.projection.n_y), 1000):
            s3.delete_objects(Delete={'Objects': [{'Key': f"{y}/{f.file_name}"} for y in ys]})
        db.session.delete(f)
        db.session.commit()

    # Now that we've removed everything we know we can, look for any files in S3
    # which aren't tracked by a FileMeta. In theory this can only happen with
    # bad code, but is worth checking for to prevent S3 usage from growing
    # unbounded.
    logger.info("Finding orphaned files to remove...")
    known_fns = set(fm.file_name for fm in FileMeta.query.all())
    to_del = []

    for obj in s3.objects.all():
        # Ignore things that are new
        if obj.last_modified >= datetime.now(obj.last_modified.tzinfo) - timedelta(hours=3):
            continue

        if os.path.basename(obj.key) not in known_fns:
            to_del.append(obj.key)

    logger.info("Removing %d orphaned files", len(to_del))
    for grp in chunk(to_del, 1000):
        s3.delete_objects(Delete={'Objects': [{'Key': key} for key in grp]})


if __name__ == "__main__":
    init_sentry()
    logging.basicConfig(level=logging.INFO)
    clean_old_datas()
