FROM rust:1.74.0

RUN rustup component add clippy rustfmt

# create non-root user for vscode
RUN groupadd --gid 1000 builder && \
    useradd --uid 1000 --gid 1000 -m builder

ENTRYPOINT ["cargo"]
