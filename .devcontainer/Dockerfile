FROM rust:1.82.0

RUN rustup component add clippy rustfmt

# install nextest
RUN curl -LsSf https://get.nexte.st/latest/linux | tar zxf - -C ${CARGO_HOME:-~/.cargo}/bin

# create non-root user for vscode
RUN groupadd --gid 1000 builder && \
    useradd --uid 1000 --gid 1000 -m builder

ENTRYPOINT ["cargo"]
