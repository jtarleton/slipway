# slipway is pure Go (modernc.org/sqlite, no cgo), so it builds fully static and
# ships on distroless/static — no shell, no package manager, just the binary and
# CA certificates.

FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOFLAGS=-trimpath go build -ldflags='-s -w' -o /slipway ./cmd/slipway

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /slipway /slipway
# The control-plane database lives here; mount a volume over it.
VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["/slipway"]
CMD ["serve", "-addr", ":8080", "-db", "/data/slipway.db"]
