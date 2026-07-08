#!/usr/bin/env bash

set -e

echo "Creating volume..."

ozone sh volume create /lakehouse \
    || true
    echo "Created volume /lakehouse"

echo "Creating buckets..."

for bucket in bronze silver gold staging; do
    ozone sh bucket create /lakehouse/$bucket \
        || true
        echo "Created bucket /lakehouse/$bucket"
done

echo "Done."