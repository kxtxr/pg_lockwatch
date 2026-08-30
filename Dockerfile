# syntax=docker/dockerfile:1

ARG PG_MAJOR=17
ARG PGRX_VERSION=0.19.2

FROM postgres:${PG_MAJOR}-bookworm AS builder

ARG PG_MAJOR
ARG PGRX_VERSION

ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    PGRX_HOME=/tmp/pgrx \
    PATH=/usr/local/cargo/bin:/usr/local/rustup/bin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        curl \
        libclang-dev \
        postgresql-server-dev-${PG_MAJOR} \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable

WORKDIR /build

COPY Cargo.toml Cargo.lock pg_lockwatch.control ./
COPY .cargo ./.cargo
COPY src ./src

RUN cargo install cargo-pgrx --version "${PGRX_VERSION}" --locked
RUN cargo pgrx init "--pg${PG_MAJOR}" "$(which pg_config)"
RUN cargo pgrx install \
    --release \
    --no-default-features \
    --features "pg${PG_MAJOR}" \
    --pg-config "$(which pg_config)"

FROM postgres:${PG_MAJOR}-bookworm

ARG PG_MAJOR

LABEL org.opencontainers.image.title="pg_lockwatch" \
      org.opencontainers.image.description="Postgres with the pg_lockwatch extension preinstalled" \
      org.opencontainers.image.source="https://github.com/kxtxr/pg_lockwatch"

COPY --from=builder /usr/lib/postgresql/${PG_MAJOR}/lib/pg_lockwatch.so /usr/lib/postgresql/${PG_MAJOR}/lib/
COPY --from=builder /usr/share/postgresql/${PG_MAJOR}/extension/pg_lockwatch.control /usr/share/postgresql/${PG_MAJOR}/extension/
COPY --from=builder /usr/share/postgresql/${PG_MAJOR}/extension/pg_lockwatch--*.sql /usr/share/postgresql/${PG_MAJOR}/extension/
COPY docker/10_pg_lockwatch.sql /docker-entrypoint-initdb.d/10_pg_lockwatch.sql
COPY docker/pg_lockwatch-entrypoint.sh /usr/local/bin/pg_lockwatch-entrypoint.sh

RUN chmod +x /usr/local/bin/pg_lockwatch-entrypoint.sh

ENTRYPOINT ["pg_lockwatch-entrypoint.sh"]
CMD ["postgres"]
