# a2a-hub: one static binary on distroless static (CA certs, tzdata, nonroot user, no shell).
# Build: podman build --format docker -f Containerfile --build-arg VERSION=<version> -t a2a-hub .
FROM --platform=$BUILDPLATFORM golang:1.26.11-trixie@sha256:fa340cb59ebeb1d53ac1cccfc334201d938fe5f3458003cccb767096d8e032b8 AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=0.0.0-dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
# distroless has no shell to make /data, so the empty directory comes from this stage.
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
		go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/a2a-hub ./cmd/a2a-hub \
	&& mkdir /out/data

FROM gcr.io/distroless/static-debian13:nonroot@sha256:aafe993665bf6200db38a02c11a9351e782a84b6a45e7f33b6695638c88e3931

# GitHub links the ghcr.io package to this repository through the source label, so the workflow token may push it.
LABEL org.opencontainers.image.source="https://github.com/orswn/a2a-hub" \
	org.opencontainers.image.description="a2a-hub: A2A 1.0 hub for agent harness sessions and external agents"

COPY --from=build /out/a2a-hub /a2a-hub
# An anonymous volume copies the owner of /data from the image, so nonroot (65532) can write to it.
COPY --from=build --chown=65532:65532 /out/data /data
ENV HUB_DATA=/data

USER nonroot
EXPOSE 8080
VOLUME /data
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ["/a2a-hub", "healthcheck"]
ENTRYPOINT ["/a2a-hub"]
