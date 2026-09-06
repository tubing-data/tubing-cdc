FROM golang:1.21-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/tubing-cdc-quickstart ./cmd/quickstart

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/tubing-cdc-quickstart /tubing-cdc-quickstart
ENTRYPOINT ["/tubing-cdc-quickstart"]
