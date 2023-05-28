open Core

type t =
  | Statement
  | Portal
[@@deriving sexp_of, variants, compare, equal]

val to_char : t -> char
val of_char : char -> t Or_error.t

include Serializable_intf.S with type t := t
