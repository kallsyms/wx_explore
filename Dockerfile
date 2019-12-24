FROM ubuntu:18.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    git curl wget vim build-essential libbz2-dev libssl-dev libreadline-dev \
    libsqlite3-dev tk-dev libpng-dev libfreetype6-dev software-properties-common\
    gdal-bin libgdal-dev libgrib-api-dev python3 python3-pip gunicorn3

RUN pip3 install --global-option=build_ext --global-option="-I/usr/include/gdal" GDAL==2.1.3 && \
    pip3 install numpy

RUN mkdir /opt/wx_explore
WORKDIR /opt/wx_explore

COPY requirements.txt /opt/wx_explore

RUN pip3 install -r requirements.txt

COPY seed.py /opt/wx_explore
COPY wx_explore /opt/wx_explore

EXPOSE 8080

CMD ["gunicorn3", "-b:8080", "wx_explore.web:app"]
