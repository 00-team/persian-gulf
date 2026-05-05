#!/bin/bash

set -e

cargo build -r -p alzahra
rm logs/alzahra*
systemctl restart alzahra

