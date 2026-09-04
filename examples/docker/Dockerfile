# Builds the flagship pipeline (crates/spate/examples/kafka_avro_to_clickhouse)
# into a minimal, non-root image. Build from the repository root:
#
#   docker build -f examples/docker/Dockerfile -t spate-pipeline .
#
# librdkafka is compiled from vendored sources (no system packages needed
# beyond the rust image's toolchain); expect the first build to take several
# minutes. For CI, layer a cargo-chef stage in front to cache dependencies.

FROM rust:1.97.1-bookworm@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97 AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p spate --example kafka_avro_to_clickhouse --features full \
    && cp target/release/examples/kafka_avro_to_clickhouse /build/pipeline

# Distroless: glibc + libgcc only, no shell, runs as nonroot (65532).
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
COPY --from=builder /build/pipeline /usr/local/bin/pipeline
COPY --from=builder /build/crates/spate/examples/kafka_avro_to_clickhouse.yaml /etc/spate/pipeline.yaml

# The config is env-interpolated (${VAR:-default}); mount a ConfigMap over
# /etc/spate/pipeline.yaml or point SPATE_CONFIG elsewhere.
ENV SPATE_CONFIG=/etc/spate/pipeline.yaml

# Admin server: /metrics, /healthz, /readyz.
EXPOSE 9090

ENTRYPOINT ["/usr/local/bin/pipeline"]
