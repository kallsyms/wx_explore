import os

class Config():
    DEBUG = os.environ.get('DEBUG', False)
    SECRET_KEY = os.environ.get('SECRET_KEY', os.urandom(32))
    POSTGRES_USER = os.environ['POSTGRES_USER']
    POSTGRES_PASS = os.environ['POSTGRES_PASS']
    POSTGRES_HOST = os.environ['POSTGRES_HOST']
    POSTGRES_DB   = os.environ['POSTGRES_DB']

    SQLALCHEMY_TRACK_MODIFICATIONS = False
