#!/bin/bash

set -e  # Exit on any error

# Build the code generator
echo "Building eventgen code generator..."
cd "$(dirname "$0")/.."

# Ensure cmd/eventgen directory exists
if [ ! -d "cmd/eventgen" ]; then
  echo "Creating cmd/eventgen directory..."
  mkdir -p cmd/eventgen
fi

# Ensure bin directory exists
if [ ! -d "bin" ]; then
  mkdir -p bin
fi

# Compile the code generator
go build -o bin/eventgen ./cmd/eventgen

# Check if build was successful
if [ $? -eq 0 ]; then
  echo "Code generator built successfully at bin/eventgen"
  echo "Usage:"
  echo "  ./bin/eventgen -schema path/to/asyncapi.json -output ./generated"
else
  echo "Build failed!"
  exit 1
fi