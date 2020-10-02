.PHONY: build clean test

build:
	dune build

clean:
	dune clean

test:
	dune runtest
