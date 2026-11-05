#!/bin/sh
set -e

# Create Docker network if it does not exist
docker network inspect orders-net >/dev/null 2>&1 || docker network create orders-net

# Create persistent database volume
docker volume create orders-db-data

# Start PostgreSQL if the container already exists
if docker container inspect orders-db >/dev/null 2>&1; then
    docker start orders-db
    docker network connect orders-net orders-db 2>/dev/null || true
else
    docker run -d \
        --name orders-db \
        --network orders-net \
        -e POSTGRES_DB=order \
        -e POSTGRES_USER=docker \
        -e POSTGRES_PASSWORD=docker \
        -p 5555:5432 \
        -v orders-db-data:/var/lib/postgresql/18/docker \
        postgres:18
fi

# Wait until PostgreSQL is ready
until docker exec orders-db pg_isready -U docker -d order >/dev/null 2>&1; do
    echo "Waiting for PostgreSQL..."
    sleep 2
done
# Build the orderservice image
docker build -t orderservice .

# Remove an existing application container, if present
docker rm -f orderservice 2>/dev/null || true

# Start the orderservice container
docker run -d \
    --name orderservice \
    --network orders-net \
    -p 3000:3000 \
    --env-file docker.env \
    orderservice

echo "Order System started successfully!"