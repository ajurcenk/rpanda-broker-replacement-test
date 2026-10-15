# MinIO and mc built from source: upstream no longer publishes images or binaries
# (minio/minio and minio/mc are 404 on Docker Hub, dl.min.io returns 410).
FROM golang:1.24.9-bookworm AS build
ARG MINIO_TAG
ARG MC_TAG
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
RUN git clone --depth 1 --branch "${MINIO_TAG}" https://github.com/minio/minio /src/minio \
 && cd /src/minio && git rev-parse HEAD > /out-minio-commit \
 && go build -trimpath -ldflags "-s -w -X github.com/minio/minio/internal/config.ReleaseTag=${MINIO_TAG}" -o /out/minio .
RUN git clone --depth 1 --branch "${MC_TAG}" https://github.com/minio/mc /src/mc \
 && cd /src/mc && git rev-parse HEAD > /out-mc-commit \
 && go build -trimpath -ldflags "-s -w" -o /out/mc .

FROM debian:12.12-slim AS minio
ARG MINIO_TAG
COPY --from=build /out/minio /usr/bin/minio
COPY --from=build /out-minio-commit /etc/minio-commit
LABEL org.opencontainers.image.version="${MINIO_TAG}"
EXPOSE 9000 9001
ENTRYPOINT ["/usr/bin/minio"]

FROM debian:12.12-slim AS mc
ARG MC_TAG
COPY --from=build /out/mc /usr/bin/mc
COPY --from=build /out-mc-commit /etc/mc-commit
LABEL org.opencontainers.image.version="${MC_TAG}"
ENTRYPOINT ["/usr/bin/mc"]
