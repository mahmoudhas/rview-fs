# rview-fs with RDMA, as a container image. Its container mounts with FUSE (/dev/fuse and
# a privileged container) and moves files over the RDMA devices it is given.
FROM ubuntu:24.04 AS build
ARG RUSTUP_VERSION=1.29.2
# static.rust-lang.org/rustup/archive/<version>/x86_64-unknown-linux-gnu/rustup-init.sha256
ARG RUSTUP_INIT_SHA256=5be191e8c70a5007074254cb74d78dbfdd25cd5a61c27de7b8e5410122ec24f4
ENV RUSTUP_HOME=/opt/rust/rustup CARGO_HOME=/opt/rust/cargo PATH=/opt/rust/cargo/bin:$PATH
# The RDMA transport builds UCX, against the verbs and rdmacm headers.
RUN apt-get update -y && apt-get install -y --no-install-recommends \
        build-essential ca-certificates curl libibverbs-dev librdmacm-dev pkg-config && \
    rm -rf /var/lib/apt/lists/*
RUN curl -fsSL --retry 3 -o /tmp/rustup-init \
        "https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/x86_64-unknown-linux-gnu/rustup-init" && \
    echo "${RUSTUP_INIT_SHA256}  /tmp/rustup-init" | sha256sum -c - && \
    chmod +x /tmp/rustup-init && \
    /tmp/rustup-init -y --no-modify-path --profile minimal --default-toolchain none && \
    rm /tmp/rustup-init
WORKDIR /src
COPY . .
# The toolchain rust-toolchain.toml pins.
RUN rustup toolchain install && cargo build --release --locked

FROM ubuntu:24.04
# fusermount3 mounts the union; the verbs and rdmacm libraries, with the mlx5 provider,
# carry RDMA.
RUN apt-get update -y && apt-get install -y --no-install-recommends \
        fuse3 ibverbs-providers libibverbs1 librdmacm1t64 && \
    rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/rview-fs /usr/local/bin/rview-fs
RUN rview-fs --help > /dev/null
ENTRYPOINT ["/usr/local/bin/rview-fs"]
