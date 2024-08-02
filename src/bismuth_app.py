# BISMUTH FILE: bismuth_app.py
from bismuth import API
import sys
sys.path.append("/repo/")
from wx_explore.web.app import app

class WxAPI(API):
    def __init__(self):
        super().__init__()
        self.app = app