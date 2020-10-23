(lang dune 2.7)

(name postgres)

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
  sexplib0
  (ppx_sexp_conv (>= "v0.14.1"))
  (alcotest :with-test)))

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
  faraday-lwt-unix
  tls))

(package
 (name postgres-mirage)
 (tags
  (postgres postgresql mirage))
 (depends
  dns-client
  ipaddr
  domain-name
  (ocaml
   (>= 4.08))
  (postgres-lwt
   (= :version))
  mirage-stack
  mirage-random
  mirage-time
  mirage-clock
  tls-mirage
  lwt))

(package
 (name pg-async)
 (tags
  (postgres postgresql mirage))
 (depends
  (async
   (>= "v0.13.0"))
  (async_ssl
   (>= "v0.13.0"))
  (core
   (>= "v0.13.0"))
  (ocaml
   (>= 4.08))
  (postgres
   (= :version))
  (ppx_jane
   (>= "v0.13.0"))))
