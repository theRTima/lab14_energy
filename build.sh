#!/bin/bash
set -e

echo "Building Rust validator library..."
cd rust
cargo build --release
cd ..

echo "Building Go binaries..."
CGO_ENABLED=1 go build -o bin/collector ./cmd/collector
CGO_ENABLED=1 go build -o bin/analyzer ./cmd/analyzer

echo "Build complete!"
echo "Binaries are in ./bin/"
echo "Rust library is in ./rust/target/release/"
