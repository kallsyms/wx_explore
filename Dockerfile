FROM ubuntu:18.04

RUN apt-get update && apt-get install -y \
    python3 python3-pip \
    gdal-bin libgdal-dev \
    libeccodes-dev \
    python3-gdal

COPY ./requirements.txt /

RUN pip3 install numpy pyproj
RUN pip3 install -r /requirements.txt
