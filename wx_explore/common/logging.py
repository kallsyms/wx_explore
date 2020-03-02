import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration

from wx_explore.common.config import Config


def init_sentry(flask=False):
    if Config.SENTRY_ENDPOINT is not None:
        sentry_sdk.init(Config.SENTRY_ENDPOINT, integrations=([FlaskIntegration()] if flask else None))
