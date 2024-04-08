#!/bin/bash

set -e
echo "building"
time go build main.go
zig build -Drelease-safe
# zig build

# echo "running go"
# time ./main
echo "running zig"
time ./zig-out/bin/main-zig

