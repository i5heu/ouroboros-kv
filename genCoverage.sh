#!/bin/bash
# Coverage script for ouroboros-kv

set -e

echo "Running tests with coverage..."
go test ./... -coverpkg=./internal/store -coverprofile=coverage.out

echo ""
echo "=== Overall Coverage ==="
go tool cover -func=coverage.out | grep "total:"

echo ""
echo "=== Coverage by File ==="
go tool cover -func=coverage.out | grep -E "^github.com/i5heu/ouroboros-kv/(cmd|internal|pkg)/" | grep -v "proto\|config" | sort -t: -k2

echo ""
echo "=== Delete Operations Coverage ==="
go tool cover -func=coverage.out | grep "delete.go"

echo ""
echo "=== List Operations Coverage ==="
go tool cover -func=coverage.out | grep "list.go"

echo ""
echo "=== HTML Coverage Report ==="
go tool cover -html=coverage.out -o coverage.html
echo "Coverage report generated: coverage.html"
echo "Open with: xdg-open coverage.html"
