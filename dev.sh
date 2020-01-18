#!/bin/sh
docker build -t kallsyms/wx_explore .
docker-compose run --rm -p 8080:8080 wx_explore /bin/bash
