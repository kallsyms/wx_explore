from wx_explore.web.core import app
from wx_explore.web.api import api
app.register_blueprint(api)

from wx_explore.common.location import preload_coordinate_lookup_meta
preload_coordinate_lookup_meta()

from wx_explore.common.seed import seed
seed()
