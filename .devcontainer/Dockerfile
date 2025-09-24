FROM rust:1.88.0

RUN rustup component add clippy rustfmt llvm-tools-preview

# install nextest
RUN curl -LsSf https://get.nexte.st/latest/linux | tar zxf - -C ${CARGO_HOME:-~/.cargo}/bin

# install tauri, see https://v2.tauri.app/start/prerequisites/
ARG TAURI=false
RUN if [ "$TAURI" = "true" ]; then \
    apt-get update && \
    apt-get install -y libwebkit2gtk-4.1-dev \
        build-essential \
        curl \
        wget \
        file \
        libxdo-dev \
        libssl-dev \
        libayatana-appindicator3-dev \
        librsvg2-dev \
        xdg-utils; \
fi

# create non-root user for vscode
ARG USERNAME=devcontainer
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    #
    # [Optional] Add sudo support. Omit if you don't need to install software after connecting.
    && apt-get update \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

USER $USERNAME

# install test coverage tools as non-root
ARG COVERAGE=false
RUN if [ "$COVERAGE" = "true" ]; then \
    cargo install cargo-llvm-cov; \
fi

# install tauri as non-root
RUN if [ "$TAURI" = "true" ]; then \
    cargo install tauri-cli --version "^2.0.0" --locked; \
fi
