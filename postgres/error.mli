type t =
  [ `Exn of exn
  | `Msg of string
  | `Sexp of Sexplib0.Sexp.t
  ]

val of_exn : exn -> t
val of_string : string -> t
val of_sexp : Sexplib0.Sexp.t -> t
val sexp_of_t : t -> Sexplib0.Sexp.t
val failf : ('a, Format.formatter, unit, ('b, t) result) format4 -> 'a
val raise : t -> _
