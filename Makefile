.PHONY: build clean test

build:
	dune build @install

clean:
	dune clean

test:
	dune runtest
