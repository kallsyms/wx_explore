#!/bin/sh

mkdir -p deploy
rm -f deploy/*.zip
rm -f __main__.py
ln -s wx_explore/cloud/functions/reduce_grib.py __main__.py
zip -r --exclude=\*__pycache__\* deploy/reduce-grib.zip wx_explore virtualenv __main__.py
rm -f __main__.py
