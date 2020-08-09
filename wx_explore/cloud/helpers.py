from sqlalchemy import create_engine
import os


def db_engine(args):
    # TODO: AWS IAM-based DB credential retrieving
    if 'CONFIG' in os.environ:
        from wx_explore.common.config import Config
        return create_engine(Config.SQLALCHEMY_DATABASE_URI)
    else:
        print("Warning: defaulting to sqlite://db.db")
        return create_engine("sqlite://db.db")
