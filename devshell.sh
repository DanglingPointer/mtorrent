#!/usr/bin/env bash

DOCKER_BUILDKIT=0 docker build -t mtorrent_builder . && \
docker run -it --rm --user "$(id -u)":"$(id -g)" -v "$PWD":/usr/src/mtorrent -w /usr/src/mtorrent --entrypoint /bin/bash mtorrent_builder
