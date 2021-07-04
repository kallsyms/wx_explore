from opentelemetry.instrumentation.flask import FlaskInstrumentor

FlaskInstrumentor().instrument()

from wx_explore.web.core import app, db
from wx_explore.web.api import api
app.register_blueprint(api)

from wx_explore.common.location import preload_coordinate_lookup_meta
preload_coordinate_lookup_meta()

# Clear so gunicorn forks with no (shared) engine
db.session.remove()
db.engine.dispose()

@app.before_first_request
def post_fork_init():
    """
    Sentry and tracing are initialized late (after gunicorn fork) so that
    any network sessions they create during init aren't shared between
    processes.
    """
    from wx_explore.common.logging import init_sentry
    from wx_explore.common.tracing import init_tracing

    init_sentry(flask=True)
    init_tracing('api')
