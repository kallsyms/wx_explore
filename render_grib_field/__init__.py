#!/usr/bin/env python3
from PIL import Image

import azure.functions as func
import base64
import io
import numpy
import pygrib
import tempfile

from wx_explore.ingest.reduce_grib import reduce_grib
from wx_explore.common.models import SourceField


def normalize(d):
    return (d - numpy.min(d)) / numpy.ptp(d)


def main(req: func.HttpRequest) -> func.HttpResponse:
    args = req.params

    if not all(param in args for param in ['grib_url', 'grib_idx_url', 'field_name', 'field_level']):
        return func.HttpResponse(
            "Missing params",
            status_code=400,
        )

    field = SourceField(
        idx_short_name=args['field_name'],
        idx_level=args['field_level'],
    )

    with tempfile.NamedTemporaryFile() as f:
        n_grib_fields = reduce_grib(args['grib_url'], args['grib_idx_url'], [field], f)
        if n_grib_fields < 1:
            return func.HttpResponse(
                "Bad field_name or field_level",
                status_code=400,
            )

        grb = pygrib.open(f.name)
        msg = grb.read(1)[0]
        data = msg.data()[0]

        img = Image.fromarray(numpy.uint8(normalize(data)*255))
        img_data = io.BytesIO()

        img.save(img_data, format='PNG')

        return func.HttpResponse(
            img_data.getvalue(),
            headers={
                "Content-Type": "image/png",
            },
        )
