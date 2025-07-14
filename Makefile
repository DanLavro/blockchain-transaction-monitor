# Blockchain Monitor Makefile

# Variables
BINARY_NAME=blockchain-monitor
MAIN_PATH=main.go
BUILD_DIR=build
DOCKER_IMAGE=blockchain-monitor
DOCKER_TAG=latest

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=gofmt
GOLINT=golangci-lint

# Build flags
LDFLAGS=-ldflags="-w -s"
BUILDFLAGS=-trimpath

.PHONY: all build clean test test-coverage lint format deps tidy run docker docker-run help

# Default target
all: clean deps test build

# Build the application
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(BUILDFLAGS) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(MAIN_PATH)
	@echo "Build completed: $(BUILD_DIR)/$(BINARY_NAME)"

# Build for Linux (useful for Docker)
build-linux:
	@echo "Building $(BINARY_NAME) for Linux..."
	@mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=amd64 $(GOBUILD) $(BUILDFLAGS) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux $(MAIN_PATH)
	@echo "Linux build completed: $(BUILD_DIR)/$(BINARY_NAME)-linux"

# Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	@rm -rf $(BUILD_DIR)
	@echo "Clean completed"

# Run tests
test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -race -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Lint code
lint:
	@echo "Running linter..."
	$(GOLINT) run ./...

# Format code
format:
	@echo "Formatting code..."
	$(GOFMT) -s -w .

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download

# Tidy dependencies
tidy:
	@echo "Tidying dependencies..."
	$(GOMOD) tidy

# Run the application
run:
	@echo "Running $(BINARY_NAME)..."
	$(GOCMD) run $(MAIN_PATH)

# Development targets
dev-setup: deps tidy
	@echo "Setting up development environment..."
	@cp .env.example .env 2>/dev/null || echo "No .env.example found"
	@echo "Development environment ready!"

# Docker targets
docker-build:
	@echo "Building Docker image..."
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

docker-run:
	@echo "Running Docker container..."
	docker run --rm -it \
		--env-file .env \
		--network blockchain-monitor_blockchain-monitor-network \
		$(DOCKER_IMAGE):$(DOCKER_TAG)

# Docker Compose targets
compose-up:
	@echo "Starting services with Docker Compose..."
	docker-compose up -d

compose-down:
	@echo "Stopping services with Docker Compose..."
	docker-compose down

compose-logs:
	@echo "Showing Docker Compose logs..."
	docker-compose logs -f

compose-test:
	@echo "Starting test environment..."
	docker-compose --profile testing up -d

# Quality checks
check: format lint test
	@echo "All quality checks passed!"

# Release build
release: clean deps test build-linux
	@echo "Release build completed!"

# Install development tools
install-tools:
	@echo "Installing development tools..."
	$(GOGET) github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Show help
help:
	@echo "Available targets:"
	@echo "  all          - Clean, download deps, test, and build"
	@echo "  build        - Build the application"
	@echo "  build-linux  - Build for Linux (Docker compatible)"
	@echo "  clean        - Clean build artifacts"
	@echo "  test         - Run tests"
	@echo "  test-coverage - Run tests with coverage report"
	@echo "  lint         - Run linter"
	@echo "  format       - Format code"
	@echo "  deps         - Download dependencies"
	@echo "  tidy         - Tidy dependencies"
	@echo "  run          - Run the application"
	@echo "  dev-setup    - Setup development environment"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-run   - Run Docker container"
	@echo "  compose-up   - Start services with Docker Compose"
	@echo "  compose-down - Stop services with Docker Compose"
	@echo "  compose-logs - Show Docker Compose logs"
	@echo "  compose-test - Start test environment"
	@echo "  check        - Run all quality checks"
	@echo "  release      - Build release version"
	@echo "  install-tools - Install development tools"
	@echo "  help         - Show this help message" 