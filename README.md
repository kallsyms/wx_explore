# Weather Explorer
(Suggestions for better names are welcome)


## What?
Weather Explorer is a website that aims to fill the gap between sites like [TWC](https://www.weather.com) which give you data, but no source
information, and detailed analysis sites like
[NOAA's analysis/model pages](http://www.spc.noaa.gov/exper/),
[PSU's e-wall](http://mp1.met.psu.edu/~fxg1/ewall.html),
and [pivotal weather](http://www.pivotalweather.com/).

It will offer an API to get point data for any given location (ZIP code, lat/lon, etc.), which will return data from
all kinds of sources (models like the HRRR, GFS, NAM, etc. as well as observational data like RADAR, CWOP, and soundings).
The primary use of the API is the main site, which gives a high-level overview of current data and a forecast (for day-to-day use),
but also shows the varience in forecast data, with a way to "dig down" to the raw source data.

For instance, the website may show a normal 5 day forecast, but each data point (temp, dew point, conditions, etc.) will
be clickable to reveal which each source predicts that data point will be. For example, tomorrows temperature may just be
shown as 50degF, but the metric is clickable to reveal that the HRRR predicts that the temperature will be 49, the GFS says 51, and the NAM says 50.
Each model run is preserved as well (i.e. not just the most recent model run is kept), so users can see trends over time.
Again, in the context of the prior example, the most recent HRRR may say 49, but the HRRR from 2 hours ago might say 50, and the HRRR from 12 hours ago might say 47.


## Why?
I haven't found a website like this, and I think it would be useful both for meteorologists (the API can be easily integrated into other projects), as well
as the average person who is just a bit curious about where their daily forecast really comes from.
