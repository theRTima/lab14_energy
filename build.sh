#!/bin/bash

echo "Building collector and analyzer..."
go build -o bin/collector ./cmd/collector
go build -o bin/analyzer ./cmd/analyzer

echo "Build complete!"
echo "Binaries are in ./bin/"
