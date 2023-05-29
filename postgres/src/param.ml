open Core

type t =
  { format_code : Format_code.t
  ; parameter : string option
  }
[@@deriving sexp_of]

let create ?parameter format_code = { format_code; parameter }
