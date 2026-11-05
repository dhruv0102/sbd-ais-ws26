# Stage 1: Build the Go application
FROM golang:1.27 AS builder

WORKDIR /app

COPY . .

RUN chmod +x scripts/build-application.sh

RUN ./scripts/build-application.sh


# Stage 2: Run the application
FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/ordersystem /app/ordersystem

EXPOSE 3000

CMD ["/app/ordersystem"]