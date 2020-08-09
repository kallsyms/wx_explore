from PIL import Image

import collections
import io
import matplotlib.colors
import numpy
import pygrib
import requests
import tempfile

from wx_explore.cloud.proxy import HttpRequest, HttpResponse, proxy

# val0, r0, g0, b0; val1, r1, g1, b1; ...
ColorMapEntry = collections.namedtuple('ColorMapEntry', ['val', 'r', 'g', 'b'])


def func(req: HttpRequest) -> HttpResponse:
    if not all(param in req.args for param in ['s3_path']):
        return HttpResponse(
            "Missing params",
            code=400,
        )

    with tempfile.NamedTemporaryFile() as f:
        with requests.get(req.args['s3_path']) as resp:
            f.write(resp.content)
            f.flush()

        grb = pygrib.open(f.name)
        msg = grb.read(1)[0]
        data = msg.data()[0]

        # flip vertically so north is up
        data = numpy.int16(data[::-1])

        if 'cm' in req.args:
            try:
                cm_data = [ColorMapEntry(*map(float, cme.split(','))) for cme in req.args['cm'].split(';')]
            except Exception:
                return HttpResponse(
                    "Malformed color map",
                    code=400,
                )

            norm = matplotlib.colors.Normalize(
                vmin=min(cme.val for cme in cm_data),
                vmax=max(cme.val for cme in cm_data),
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

            if req.args.get('cm_mask_under'):
                cm.set_under(alpha=0)

            img = Image.fromarray(numpy.uint8(cm(norm(data))*255), 'RGBA')
        else:
            img = Image.fromarray(data)

        img_data = io.BytesIO()
        img.save(img_data, format='PNG')

        return HttpResponse(
            img_data.getvalue(),
            headers={
                "Content-Type": "image/png",
            },
        )


main = proxy(func)
