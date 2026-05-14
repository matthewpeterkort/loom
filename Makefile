.PHONY: all build check test fmt run-server clean

all: build

build:
	cargo build --workspace

check:
	cargo check --workspace

test:
	cargo test --workspace

fmt:
	cargo fmt --all

run-server:
	cargo run -p loom-server

clean:
	cargo clean
