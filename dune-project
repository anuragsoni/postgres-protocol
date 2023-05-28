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
  (ppx_sexp_conv
   (>= v0.15.0))
  (ppx_let
   (>= v0.15.0))
  mirage-crypto-rng
  base64
  pbkdf
  (ppx_jane :with-test)))
