#!/usr/bin/env bash
set -eu

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
URL="https://zenodo.org/records/6382482/files/debs2022-gc-trading-day-12-11-21.csv?download=1"
DEST="$ROOT/data/src/debs2022-gc-trading-day-12-11-21.csv"

mkdir -p -- "$(dirname -- "$DEST")"
TEMP=$(mktemp "${DEST}.XXXXXX")
trap 'rm -f -- "$TEMP"' EXIT
curl --fail --location --output "$TEMP" "$URL"
# The non-root replay container needs read access to the mounted file.
chmod a+r "$TEMP"
mv -- "$TEMP" "$DEST"
printf 'Downloaded %s\n' "$DEST"
