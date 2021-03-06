#!/usr/bin/env bash

cd prog
protoc -I=. --go_out=. ./command.proto
protoc -I=. --go-grpc_out=. ./command.proto