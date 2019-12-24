#!/bin/sh
docker build -t kallsyms/wx_explore .
docker-compose run -d db
docker-compose run --rm wx_explore /bin/bash
