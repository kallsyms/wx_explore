* Locations
    * ID, lat & lon, precomputed x & y for each dataset, name
    * Search by
        * Name
        * lat & lon
* Backend storage
    * Dump all gridded data to postgis raster
        * Benefits:
            * Replication
            * Perf
            * Native image rendering (`ST_As{JPEG|PNG|TIFF}`)
        * Drawbacks
            * ?
    * Use `ST_Value` with the pre-computed x & y
