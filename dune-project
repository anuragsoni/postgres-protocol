(lang dune 3.7)

(name postgres)

(generate_opam_files true)

(source
 (github anuragsoni/postgres-protocol))

(authors "Anurag Soni")

(maintainers "Anurag Soni <anurag@sonianurag.com>")

(license MPL-2.0)

(package
 (name postgres)
 (tags
  (postgres postgresql))
 (depends
  (ocaml
   (>= 4.14))
  (core
   (>= v0.15.0))
  (core_kernel
   (>= v0.15.0))
  (ppx_jane
   (>= v0.15.0))
  mirage-crypto-rng
  base64
  pbkdf))

(package
 (name postgres_eio)
 (tags
  (postgres postgresql eio))
 (depends
  (ocaml
   (>= 5.0.0))
  eio
  (postgres
   (= :version))))
