#!/bin/bash -e

pkill aerodis || true

echo "Standard test"
../aerodis --config_file config.json &
sleep 3
php test.php
pkill aerodis || true
sleep 3

echo "Expanded map test"
../aerodis --config_file config_expanded_map.json &
sleep 3
php test.php
pkill aerodis || true
sleep 3
