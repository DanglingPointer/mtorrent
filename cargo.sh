#!/usr/bin/env bash

docker build -t mtorrent_builder .devcontainer/ && \
docker run --rm --user "$(id -u)":"$(id -g)" -v "$PWD":/usr/src/mtorrent -w /usr/src/mtorrent --entrypoint /usr/local/cargo/bin/cargo mtorrent_builder "$@"
