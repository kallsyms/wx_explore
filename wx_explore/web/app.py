from opentelemetry.ext.flask import FlaskInstrumentor

FlaskInstrumentor().instrument()

from wx_explore.common.logging import init_sentry
init_sentry(flask=True)

from wx_explore.web.core import app

from wx_explore.common.tracing import init_tracing
init_tracing('api')

from wx_explore.web.api import api
app.register_blueprint(api)

from wx_explore.common.location import preload_coordinate_lookup_meta
preload_coordinate_lookup_meta()
