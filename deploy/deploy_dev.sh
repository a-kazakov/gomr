#!/usr/bin/env bash

set -exuo pipefail

export AWS_PROFILE="cmbe-push"

ECR_URL="<DEV_ECR_URL_PLACEHOLDER>"
ECR_HOST="<DEV_ECR_HOST_PLACEHOLDER>"

FILE_DIR=$(dirname "$0")
trap 'popd' EXIT
pushd "${FILE_DIR}"/../

aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin "$ECR_HOST"
docker buildx build . -f deploy/Dockerfile --tag "$ECR_URL:latest" --platform linux/amd64,linux/arm64 --push --compress
