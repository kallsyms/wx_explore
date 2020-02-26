# Quick list
* ensemble ingest
* confidence on summarized metrics
* debouncer
    * tag events as scattered or something if debouncing kicks in

* wind direction grib field is "from which blowing" - derived is where to?

* add timezone to location table
* batch worker imports by source so DB upserts are more efficient

* frontend
    * unit conversions
* api
    * metrics, sources: change result to dict keyed off of id

# Medium term
* incorporate l2 radar into rta
    * streaming from https://registry.opendata.aws/noaa-nexrad/

# Long term misc ideas
* Give each storm cell a unique ID (will already need to be identified for rta); plot trajectory over time, aggregate stats about where cells pop up, etc.

# Stage 1: API
* Missing data values
* Figure out best representation to return metric data in API
* Transformers on ingest to standardize units
* Cloud!
* More data sources
    * [GEFS/GENS](https://www.nco.ncep.noaa.gov/pmb/products/gens/)
    * [RAP](http://www.nco.ncep.noaa.gov/pmb/products/rap/)
    * [SREF](http://www.nco.ncep.noaa.gov/pmb/products/sref/)
    * [CMCE](http://www.nco.ncep.noaa.gov/pmb/products/cmcens/)
    * [UKMET](http://www.nco.ncep.noaa.gov/pmb/products/ukmet/)
    * METAR (CWOP?)
    * NEXRAD
    * Satellite
    * Soundings
    * Anything else from noaaport?

* Maps
    * May be tricky since we store `raster`s per row. Would have to recombine in DB, or we would have to transfer all of the sub-`raster`s from the DB to the frontend which could be quite a bit of data.
        * Recombining in DB with `ST_Union` seems to be too slow to use this for real-time requests (>5sec for a HRRR temperature grid)
        * We would probably be pre-computing the maps for each band anyways...
* Do we keep the raw grib files for people to download?
    * Probably...
* Parallelize ingest (thread to download all gribs, threads to ingest each)

# Stage 2: WWW
* Accounts
* Location saving (account or cookie) in web interface
* Docs/explanations of sources
