#!/usr/bin/env python3
from sqlalchemy.orm import sessionmaker
import tempfile

from wx_explore.cloud.proxy import HttpRequest, HttpResponse, proxy
from wx_explore.cloud.helpers import s3_client, db_engine
from wx_explore.common.models import Source, SourceField
from wx_explore.ingest.reduce_grib import reduce_grib


def func(req):
    if not all(param in req.args for param in ['url', 'idx', 'source_name', 'out']):
        raise ValueError("Missing params")

    s3 = s3_client(req.args)
    engine = db_engine(req.args)
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()

    fields = (session.query(SourceField).join(Source, SourceField.source_id == Source.id)
              .filter(Source.name == req.args['source_name'])
              .all())

    if not fields:
        raise ValueError("Provided source_name does not have a Source table entry")

    with tempfile.TemporaryFile() as f:
        reduce_grib(req.args['url'], req.args['idx'], fields, f)
        f.seek(0)
        s3.upload_fileobj(f, "vtxwx-data", req.args['out'], ExtraArgs={'ACL': 'public-read'})

    return HttpResponse(
        {"status": "ok", "filename": req.args['out']}
    )

main = proxy(func)
