#!/bin/bash

set -e

git pull
cargo build -r -p alzahra
rm logs/alzahra*
systemctl restart alzahra

