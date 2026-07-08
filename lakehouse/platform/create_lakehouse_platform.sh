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

echo "Creating s3v volume for S3 Gateway access..."

ozone sh volume create /s3v \
    || true
echo "Created volume /s3v"

echo "Linking buckets into s3v..."

for bucket in bronze silver gold staging; do
    ozone sh bucket link /lakehouse/$bucket /s3v/$bucket \
        || true
    echo "Linked /s3v/$bucket -> /lakehouse/$bucket"
done

echo "Done."