FROM rust:1.74.0

RUN rustup component add clippy rustfmt

# create non-root user so that the compiled binary won't be owned by root
RUN groupadd --gid 1000 builder && \
    useradd --uid 1000 --gid 1000 -m builder

# switch to non-root before creating workdir
USER builder

WORKDIR /home/builder/src/mtorrent
COPY . .

ENTRYPOINT ["cargo"]
