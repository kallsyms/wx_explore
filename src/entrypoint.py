# BISMUTH FILE: entrypoint.py
# ENTRYPOINT: bismuth_app.py|WxAPI
from bismuth_app import WxAPI

app = WxAPI()

if __name__ == "__main__":
    app.run()