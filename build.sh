#!/usr/bin/env bash

docker build -t mybuilder . && \
docker run --rm -v "$PWD":/home/builder/src/mtorrent mybuilder build --release --all-targets
