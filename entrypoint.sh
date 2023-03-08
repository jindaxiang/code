#!/bin/sh

echo "Waiting for redis..."

while ! nc -z web-redis 6379; do
	  sleep 2
  done

  echo "Redis started"
  
  echo "Data init started"
  
  python3 time_save.py

  exec "$@"
