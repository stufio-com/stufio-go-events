#!/bin/bash

# Build the code generator
echo "Building eventgen code generator..."
cd "$(dirname "$0")/.."
go build -o bin/eventgen ./cmd/eventgen

echo "Code generator built at bin/eventgen"
echo "Usage:"
echo "  ./bin/eventgen -schema path/to/asyncapi.json -output ./generated"