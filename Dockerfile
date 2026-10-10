# syntax=docker/dockerfile:1

# Build: web/ is embedded into the binary (go:embed), so the image only needs it.
FROM golang:1.24-alpine AS build
WORKDIR /src
ENV GOWORK=off CGO_ENABLED=0
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY web ./web
RUN go build -trimpath -ldflags="-s -w" -o /out/pipeline-report .

FROM alpine:3.20
# tzdata for REPORT_TZ (Asia/Yangon); wget (busybox) for the healthcheck.
RUN apk add --no-cache tzdata \
 && adduser -D -H -u 10001 report
COPY --from=build /out/pipeline-report /usr/local/bin/pipeline-report
USER report
# The container always listens on :8090; pick the host port when publishing.
ENV REPORT_HTTP_ADDR=:8090
EXPOSE 8090
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8090/healthz >/dev/null || exit 1
# Config comes from environment variables only (no .env inside the image).
ENTRYPOINT ["pipeline-report", "--env", ""]
