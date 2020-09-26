.PHONY: build clean test deps

build:
	dune build @install

clean:
	dune clean

test:
	dune runtest

deps:
	opam pin add gluten --dev-repo
	opam pin add gluten-lwt --dev-repo
	opam pin add gluten-lwt-unix --dev-repo
	opam install angstrom faraday logs
