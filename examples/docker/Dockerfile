# Builds the flagship pipeline (crates/etl/examples/kafka_avro_to_clickhouse)
# into a minimal, non-root image. Build from the repository root:
#
#   docker build -f examples/docker/Dockerfile -t etl-pipeline .
#
# librdkafka is compiled from vendored sources (no system packages needed
# beyond the rust image's toolchain); expect the first build to take several
# minutes. For CI, layer a cargo-chef stage in front to cache dependencies.

FROM rust:1.96-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p etl --example kafka_avro_to_clickhouse --features full \
    && cp target/release/examples/kafka_avro_to_clickhouse /build/pipeline

# Distroless: glibc + libgcc only, no shell, runs as nonroot (65532).
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /build/pipeline /usr/local/bin/pipeline
COPY --from=builder /build/crates/etl/examples/kafka_avro_to_clickhouse.yaml /etc/etl/pipeline.yaml

# The config is env-interpolated (${VAR:-default}); mount a ConfigMap over
# /etc/etl/pipeline.yaml or point ETL_CONFIG elsewhere.
ENV ETL_CONFIG=/etc/etl/pipeline.yaml

# Admin server: /metrics, /healthz, /readyz.
EXPOSE 9090

ENTRYPOINT ["/usr/local/bin/pipeline"]
