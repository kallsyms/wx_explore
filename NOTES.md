# Random Notes From Development

## How does ingesting work
1. Get GRIB idx, and concat the pieces of the GRIB needed to reduce bandwidth and some computation time
2. For each message in the GRIB:
    1. Ensure that a location->x,y LUT exists. If not, create it.
    2. For each row in the message, extract that row as a `raster` and insert it into the DB keeping track of the source metric, the valid timestamp, and the row index

### Reprojection?
I'm trying to keep reprojecting things down to a minimum for a couple of reasons. First, accuracy. The fewer reprojections, the less data interpolation which makes it easier to see exactly where the data originated from (a key part of the project.). Second, reprojecting rasters takes a **long** time. It's much easier to just project single points that are needed for the main API, especially since we can build LUTs ahead of time which allow us to directly map from arbitrary coordinates to an x,y in the grid which is even faster than any actual coordinate reprojection could be.

## Basic Models (Classes)
* Metric - e.g. 2m temperature, dew point, raining, snowing
    * Name (2m Temperature)
    * Unit (degC)
* Source - HRRR, GFS, NAM, CWOP, NEXRAD
    * Name (HRRR)
    * URL
    * Last updated
* Source Field - HRRR 2m temps
    * Source (HRRR)
    * Metric (2m Temperature)
    * Details that the ingest service needs to know so that it can pull the proper GRIB bands out
* Location
    * Name (10001)
    * Point (whatever coords correspond to the center of the 10001 zipcode)
* Data Point/Raster
    * Source Field (HRRR 2m temps)
    * Valid time (2017-12-31 01:00:00)
    * Run time (2017-12-31 00:00:00)
    * The raster/data itself
