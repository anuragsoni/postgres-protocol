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
 (name postgres)
 (tags
  (postgres postgresql))
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
 (name postgres-lwt)
 (tags
  (postgres postgresql))
 (depends
  (ocaml
   (>= 4.08))
  (postgres
   (= :version))
  lwt))

(package
 (name postgres-lwt-unix)
 (tags
  (postgres postgresql))
 (depends
  (ocaml
   (>= 4.08))
  (postgres-lwt
   (= :version))
  lwt
  faraday-lwt-unix))
