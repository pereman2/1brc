#!/bin/bash

set -e
echo "building"
time go build main.go
echo "running"
time ./main

