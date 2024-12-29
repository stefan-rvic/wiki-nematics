# Step 1: Use a lightweight Go base image
FROM golang:1.20 as builder

# Set the working directory inside the container
WORKDIR /app

# Copy Go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code into the container
COPY . .

# Build the Go binary
RUN go build -o kafka-client main.go

# Step 2: Use a smaller base image for the final container
FROM debian:bullseye-slim

# Install any required CA certificates
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the compiled Go binary from the builder stage
COPY --from=builder /app/kafka-client /usr/local/bin/kafka-client

# Expose any necessary ports (optional)
EXPOSE 8080

# Set the default command to run the Kafka client
CMD ["kafka-client"]
