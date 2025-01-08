# Step 1: Use the latest Go base image for building the binary
FROM golang:1.23 AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy Go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code into the container
COPY . .

# Build the Go binary statically to avoid runtime dependencies
RUN CGO_ENABLED=0 go build -o kafka-client .

# Step 2: Use a minimal base image for the final container
FROM debian:bullseye-slim

# Install any required CA certificates
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the compiled Go binary from the builder stage
COPY --from=builder /app/kafka-client /usr/local/bin/kafka-client

# Cloud Run will provide the PORT environment variable
ENV PORT=8080
ENV KAFKA_BROKERS="bootstrap.wiki-nematics.europe-west9.managedkafka.wiki-nematics.cloud.goog:9092"

# Expose the port that Cloud Run will use
EXPOSE ${PORT}

# Set the default command to run the Kafka client
CMD ["kafka-client"]
