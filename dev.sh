#!/bin/sh
export COMPOSE_FILE=docker-compose.dev.yaml
docker-compose up --no-start && docker-compose start
docker-compose exec wx_explore /opt/wx_explore/seed.py
docker-compose exec wx_explore /bin/bash
