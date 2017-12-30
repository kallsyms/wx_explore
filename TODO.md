# Stage 1: API
* Missing data values
* Figure out best representation to return metric data in API
* Transformers on ingest to standardize units

* More data sources
    * ~HRRR~
    * GFS
    * NAM
    * CWOP
    * NEXRAD
    * Satellite
    * Soundings
    * Anything else from noaaport
* Caching
* Maps
    * May be tricky since we store `raster`s per row. Would have to recombine in DB, or we would have to transfer all of the sub-`raster`s from the DB to the frontend which could be quite a bit of data.

# Stage 2: WWW
* Move front-end to static pages that use API
* Accounts
* Location saving (account or cookie) in web interface
* Docs/explanations of sources
