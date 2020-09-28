(lang dune 2.7)

(name postgres-protocol)

(generate_opam_files true)

(source
 (github anuragsoni/postgres-protocol))

(authors "Anurag Soni")

(maintainers "Anurag Soni <anurag@sonianurag.com>")

(documentation https://anuragsoni.github.io/postgres-protocol)

(license BSD-3-clause)

(package
 (name postgres-protocol)
 (tags (postgres))
 (depends
  (ocaml
   (>= 4.08))
  (angstrom
   (>= 0.14.1))
  (faraday
   (>= 0.7.2))
  logs
  (alcotest :with-test)
  (qcheck-alcotest :with-test)))

(package
 (name postgres-protocol-lwt-unix)
 (tags (postgres))
 (depends
  (ocaml
   (>= 4.08))
  (postgres-protocol
   (= :version))
  lwt
  gluten-lwt-unix
  tls))
