#!/bin/bash

protoc -I order order/orderpb/order.proto --go_out=plugins=grpc:order
