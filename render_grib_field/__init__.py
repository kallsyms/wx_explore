#!/usr/bin/env python3
from PIL import Image

import azure.functions as func
import base64
import collections
import io
import matplotlib.colors
import numpy
import pygrib
import tempfile

from wx_explore.ingest.reduce_grib import reduce_grib
from wx_explore.common.models import SourceField


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
        try:
            msg = grb.read(1)[0]
        except OSError:
            return func.HttpResponse(
                "Potentially mismatched idx url",
                status_code=400,
            )

        data = msg.data()[0]

        # flip vertically so north is up
        data = numpy.int16(data[::-1])

        if 'cm' in args:
            # val0, r0, g0, b0; val1, r1, g1, b1; ...
            ColorMapEntry = collections.namedtuple('ColorMapEntry', ['val', 'r', 'g', 'b'])

            try:
                cm_data = [ColorMapEntry(*map(float, cme.split(','))) for cme in args['cm'].split(';')]
            except:
                return func.HttpResponse(
                    "Malformed color map",
                    status_code=400,
                )

            norm = matplotlib.colors.Normalize(
                vmin=min([cme.val for cme in cm_data]),
                vmax=max([cme.val for cme in cm_data]),
            )

            cdict = {
                'red': [(norm(cme.val), cme.r/255.0, cme.r/255.0) for cme in cm_data],
                'green': [(norm(cme.val), cme.g/255.0, cme.g/255.0) for cme in cm_data],
                'blue': [(norm(cme.val), cme.b/255.0, cme.b/255.0) for cme in cm_data],
            }

            cm = matplotlib.colors.LinearSegmentedColormap(
                'cm',
                cdict,
            )

            if args.get('cm_mask_under'):
                cm.set_under(alpha=0)

            img = Image.fromarray(numpy.uint8(cm(norm(data))*255), 'RGBA')
        else:
            img = Image.fromarray(data)

        img_data = io.BytesIO()

        img.save(img_data, format='PNG')

        return func.HttpResponse(
            img_data.getvalue(),
            headers={
                "Content-Type": "image/png",
            },
        )
