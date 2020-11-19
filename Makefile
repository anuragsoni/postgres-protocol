.PHONY: build clean test fmt

build:
	dune build

clean:
	dune clean

test:
	dune runtest

fmt:
	dune build @fmt --auto-promote
