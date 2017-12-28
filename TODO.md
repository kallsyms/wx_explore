# Stage 1: API
* More data sources
    * HRRR, GFS, NAM, CWOP, NEXRAD, Satellite
    * Probably rewrite ingestor as a bash one-liner?
        * wget -> raster2pgsql (select bands from DB, transform?) -> psql
* Figure out best representation to return metric data in API
* Caching

# Stage 2: WWW
* Move front-end to static pages that use API
* Location saving in web interface
* Maps/nbsp integration