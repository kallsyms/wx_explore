# Real-Time Analysis (RTA)

This directory contains anything and everything related to streaming (real-time) analysis of new model data,
and, possibly more importantly, code to do high-resolution time interpolation of models (1m or so).

Right now there are 4 ways I'm exploring to do time interpolation in a classic time/accuracy trade-off:


## 1. Simple linear interpolation (wgrib2).

This just smoothly transitions from a "frame" at one time to a frame at another.
This is probably appropriate for "smooth" fields (wind, temp, etc.) but does not produce good results for fields which may have very defined features (radar, cloud cover, etc.) as cells don't move, they just slowly fade from their original position and fade in at the new position.

TODO:
* Deploy the wgrib-static binary as a cloud function which is invoked on all smooth fields for each model run


## 2. Wind motion based interpolation (python)

This is an experiment to see if wind data can be used to "drag" radar/satellite/etc. products from one frame to the next.

Issues:

* Can produce big jumps between where the wind simulation ends and where the next model frame is.

TODO:

* Switch to using high-altitude wind fields


## 3. Movement based interpolation (python - TBD)

Hypothetically we could determine, for each cloud/radar cluster, where it moves to in the next frame (find the average movement of the entire image and then find nearest neighbor based on the predicted position from the average) and then linearly step between the two.

Issues:

* Unclear how well the general idea of identifying where a given cell moves to would work
* Can't deal with cells popping up/dying in between time steps

TODO:

* Figure out how to do all of this


## 4. Full numerical simulation (UEMS/wrf-arw)

Just model it ourselves. Since we have end conditions, we can nudge the simulation to that end which should reduce/remove jumps from our simulation to the next frame.

Issues:

* Time. Even on a big EC2 instance (c5.4xlarge) a single 3 hour global GFS simulation (with a 240 second timestep nonetheless) takes ~5 minutes.
