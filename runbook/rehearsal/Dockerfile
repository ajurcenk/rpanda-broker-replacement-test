# Bastion image for rehearsing the runbook against the Docker test harness.
# Same tools a production bastion needs: bash 5, rpk 25.3.17, jq, curl, dig.
# (The aws CLI is replaced by rehearsal/aws, mounted at run time.)
FROM debian:12.12-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends bash jq curl dnsutils procps ca-certificates less \
 && rm -rf /var/lib/apt/lists/*
COPY --from=docker.redpanda.com/redpandadata/redpanda:v25.3.17 /opt/redpanda/libexec/rpk /usr/local/bin/rpk
WORKDIR /runbook
CMD ["sleep", "infinity"]
