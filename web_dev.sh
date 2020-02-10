#!/bin/sh
export LC_ALL=C.UTF-8
export LANG=C.UTF-8
FLASK_APP="wx_explore.web.app" python3 -m flask run -p 8080 -h 0.0.0.0
