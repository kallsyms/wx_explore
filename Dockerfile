FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    python3 python3-pip gunicorn \
    libeccodes-dev gdal-bin libgdal-dev

RUN mkdir /opt/wx_explore
WORKDIR /opt/wx_explore

COPY requirements.txt /opt/wx_explore

RUN pip3 install -r requirements.txt

COPY seed.py /opt/wx_explore
COPY wx_explore /opt/wx_explore/wx_explore
COPY data /opt/wx_explore/data

EXPOSE 8080

CMD ["gunicorn3", "-b:8080", "--preload", "--workers=4", "wx_explore.web.app:app"]
