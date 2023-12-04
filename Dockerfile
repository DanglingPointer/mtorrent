FROM rust:1.74.0

# install rust linter and formatter
RUN rustup component add clippy rustfmt

ENTRYPOINT ["cargo"]
