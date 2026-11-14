# a2a-hub: one static binary on distroless static (CA certs, tzdata, nonroot user, no shell).
# Build: make build && podman build --format docker -f Containerfile -t a2a-hub .
FROM gcr.io/distroless/static-debian13:nonroot@sha256:aafe993665bf6200db38a02c11a9351e782a84b6a45e7f33b6695638c88e3931

# GitHub links the ghcr.io package to this repository through the source label, so the workflow token may push it.
LABEL org.opencontainers.image.source="https://github.com/orswn/a2a-hub" \
	org.opencontainers.image.description="a2a-hub: A2A 1.0 hub for agent harness sessions and external agents"

COPY bin/a2a-hub /a2a-hub
ENV HUB_DATA=/data

USER nonroot
EXPOSE 8080
VOLUME /data
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ["/a2a-hub", "healthcheck"]
ENTRYPOINT ["/a2a-hub"]
