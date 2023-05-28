open Core

type t =
  | Statement
  | Portal
[@@deriving sexp_of, variants, compare, equal]

let of_char = function
  | 'S' -> Ok Statement
  | 'P' -> Ok Portal
  | ch -> Or_error.errorf "Expected Statement('S') or Portal('P') but received: %C" ch
;;

let to_char = function
  | Statement -> 'S'
  | Portal -> 'P'
;;

let to_write _ = 1

let write t buf =
  assert (Iobuf.length buf >= 1);
  Iobuf.Fill.char buf (to_char t)
;;

