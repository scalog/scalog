#!/bin/bash

protoc -I order order/orderpb/order.proto --go_out=plugins=grpc:order
protoc -I data data/datapb/data.proto --go_out=plugins=grpc:data
