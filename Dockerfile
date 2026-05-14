FROM rust:1-bookworm AS builder

WORKDIR /app

# Avoid host-specific CPU flags from local cargo config when building images.
ENV RUSTFLAGS=""

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        clang \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --release --locked -p loom-server

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libgcc-s1 \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mkdir -p /var/lib/loom /var/tmp/loom

COPY --from=builder /app/target/release/loom-server /usr/local/bin/loom-server

EXPOSE 8080 50051

ENV LOOM_HTTP_HOST=0.0.0.0
ENV LOOM_HTTP_PORT=8080
ENV LOOM_FLIGHT_HOST=0.0.0.0
ENV LOOM_FLIGHT_PORT=50051
ENV LOOM_WORK_DIR=/var/tmp/loom
ENV LOOM_DATA_DIR=/var/lib/loom

VOLUME ["/var/lib/loom"]

CMD ["loom-server"]
