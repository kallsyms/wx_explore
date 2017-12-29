# Random Notes From Development

## Why reproject at import time?
PostGIS doesn't seem to have an easy way to do on-the-fly reprojection of a point (the location queried) to a non-EPSG system.
HRRR and GFS (maybe NAM too?) don't come in a standard projection, so we would need to compute the projected location Python-side for each projection we see in the DB, which means many DB round trips.

## Why not project from location to x,y?
I tried that and it was very slow. The projection from location to x,y is fast, but `ST_Value` was being very slow. In theory it should be just a couple of disk accesses, but I'm guessing there was some compression going on Postgres-side which meant it had to somehow decode each `raster` before doing the seek.