#!/bin/bash

set -e
echo "building"
time go build main.go
zig build -Doptimize=ReleaseFast
# zig build

# echo "running go"
# time ./main
echo "running zig"
time ./zig-out/bin/main-zig

