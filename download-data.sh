#!/usr/bin/env bash

DATA_PATH=./data/src

set -euo pipefail

mkdir -p ./data/src

download_data() {
    wget -O $DATA_PATH/part.csv --header="Range: bytes=0-9999999" "https://zenodo.org/records/6382482/files/debs2022-gc-trading-day-08-11-21.csv?download=1"
}

download_data
