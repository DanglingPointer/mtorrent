FROM rust:1.88.0

RUN rustup component add clippy rustfmt llvm-tools-preview

# install nextest
RUN curl -LsSf https://get.nexte.st/latest/linux | tar zxf - -C ${CARGO_HOME:-~/.cargo}/bin

# create non-root user for vscode
RUN groupadd --gid 1000 builder && \
    useradd --uid 1000 --gid 1000 -m builder

# install test coverage tools
USER builder
RUN cargo install cargo-llvm-cov
