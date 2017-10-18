#!/bin/bash -e

pkill aerodis || true
pkill redis-server || true

echo "Redis test"
redis-server &
sleep 3
USE_REAL_REDIS=1 php test.php
pkill redis-server || true
sleep 3
