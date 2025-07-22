# Makefile
SHELL := /bin/bash
.PHONY: default
default: build

cd-backend-cpp-dir:
	cd ${BACKEND-RUST-DIR}

build:
	docker build -f Dockerfile --tag public.ecr.aws/b8h3z2a1/sravz/backend-rust:$(BACKEND_RUST_IMAGE_Version) .

