#!/bin/bash

docker build --tag phact/s3-diag ./
docker push phact/s3-diag:latest
