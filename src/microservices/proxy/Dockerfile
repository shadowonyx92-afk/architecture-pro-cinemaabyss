# ---------- build stage ----------
FROM golang:1.23-alpine AS builder

WORKDIR /app

# зависимости
COPY go.mod go.sum ./
RUN go mod download

# исходники
COPY . .

# сборка бинаря
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o proxy-service

# ---------- runtime stage ----------
FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=builder /app/proxy-service /proxy-service

EXPOSE 8000

USER nonroot:nonroot

ENTRYPOINT ["/proxy-service"]
