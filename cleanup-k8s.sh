#!/bin/bash
set -e

echo "=== Cleaning up Energy Monitoring System ==="

kubectl delete -f k8s/ --ignore-not-found=true

echo ""
echo "Waiting for resources to be deleted..."
kubectl wait --for=delete namespace/energy-monitoring --timeout=60s 2>/dev/null || true

echo ""
echo "Cleanup complete!"
