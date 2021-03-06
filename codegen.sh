#!/usr/bin/env bash

# This script serves as an example to demonstrate how to generate the gRPC-Go
# interface and the related messages from .proto file.
#
# It assumes the installation of i) Google proto buffer compiler at
# https://github.com/google/protobuf (after v2.6.1) and ii) the Go codegen
# plugin at https://github.com/golang/protobuf (after 2015-02-20). If you have
# not, please install them first.
#
# We recommend running this script at $GOPATH/src.
#
# If this is not what you need, feel free to make your own scripts. Again, this
# script is for demonstration purpose.
#
protoc --go_out=plugins=grpc:. proto/common.proto
protoc --go_out=plugins=grpc:. --proto_path proto proto/txn.proto
protoc --go_out=plugins=grpc:. --proto_path proto proto/kvcc.proto
protoc --go_out=plugins=grpc:. --proto_path  proto proto/oracle.proto
protoc --go_out=plugins=grpc:. --proto_path  proto proto/sp_kv.proto
find . -iname "*.pb.go" | xargs -I {} sed -i 's/"proto\/commonpb"/"github.com\/leisurelyrcxf\/spermwhale\/proto\/commonpb"/g' {}
