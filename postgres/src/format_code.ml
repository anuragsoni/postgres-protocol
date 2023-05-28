open Core

type t =
  | Text
  | Binary
[@@deriving sexp_of, variants, compare, equal]

let of_int = function
  | 0 -> Ok Text
  | 1 -> Ok Binary
  | num -> Or_error.errorf "Invalid format code: %d" num
;;

let to_int = function
  | Text -> 0
  | Binary -> 1
;;

let to_write _ = 2

let write t buf =
  assert (Iobuf.length buf >= 2);
  Iobuf.Fill.int16_be_trunc buf (to_int t)
;;
