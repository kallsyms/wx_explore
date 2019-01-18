# Quick list
* Move to `bootstrap_flask` or to plain old bootstrap
    * Or off entirely to Vue...

# Stage 1: API
* Missing data values
* Figure out best representation to return metric data in API
* Transformers on ingest to standardize units
* Cloud!
* More data sources
    * ~[HRRR](http://www.nco.ncep.noaa.gov/pmb/products/hrrr/)~
    * [RAP](http://www.nco.ncep.noaa.gov/pmb/products/rap/)
    * [GFS](http://www.nco.ncep.noaa.gov/pmb/products/gfs/)
    * [NAM](http://www.nco.ncep.noaa.gov/pmb/products/nam/)
    * [SREF](http://www.nco.ncep.noaa.gov/pmb/products/sref/)
    * [RTMA](http://www.nco.ncep.noaa.gov/pmb/products/rtma/)
    * [CMCE](http://www.nco.ncep.noaa.gov/pmb/products/cmcens/)
    * [UKMET](http://www.nco.ncep.noaa.gov/pmb/products/ukmet/)
    * METAR (CWOP?)
    * NEXRAD
    * Satellite
    * Soundings
    * Anything else from noaaport?

* Caching
* Maps
    * May be tricky since we store `raster`s per row. Would have to recombine in DB, or we would have to transfer all of the sub-`raster`s from the DB to the frontend which could be quite a bit of data.
        * Recombining in DB with `ST_Union` seems to be too slow to use this for real-time requests (>5sec for a HRRR temperature grid)
        * We would probably be pre-computing the maps for each band anyways...
* Do we keep the raw grib files for people to download?
    * Probably...
* Parallelize ingest (thread to download all gribs, threads to ingest each)

# Stage 2: WWW
* Move front-end to static pages that use API
* Accounts
* Location saving (account or cookie) in web interface
* Docs/explanations of sources
