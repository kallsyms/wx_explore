#!/usr/bin/env python3
from datetime import datetime, timedelta
import logging

from wx_explore.common.logging import init_sentry
from wx_explore.common.models import (
    FileMeta,
    FileBandMeta,
)
from wx_explore.common.storage import get_s3_bucket, session_allocator
from wx_explore.web.core import db

logger = logging.getLogger(__name__)


def clean_old_datas():
    oldest_time = datetime.utcnow() - timedelta(days=1)

    FileBandMeta.query.filter(FileBandMeta.valid_time < oldest_time).delete()
    db.session.commit()

    files = FileMeta.query.filter(
        FileMeta.file_name.notin_(FileBandMeta.query.with_entities(FileBandMeta.file_name)),
        FileMeta.ctime <= datetime.utcnow() - timedelta(hours=1),  # make sure we don't delete files being populated right now
    ).all()

    with session_allocator.get_session() as s:
        s3 = get_s3_bucket(s)

        for f in files:
            logger.info("Removing stale file group %s", f.file_name)
            s3.delete_objects(Delete={'Objects': [{'Key': f"{y}/{f.file_name}"} for y in range(f.projection.n_y)]})
            db.session.delete(f)
            db.session.commit()


if __name__ == "__main__":
    init_sentry()
    logging.basicConfig(level=logging.INFO)
    clean_old_datas()
