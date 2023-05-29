type t =
  { format_code : Format_code.t
  ; parameter : string option
  }
[@@deriving sexp_of]

val create : ?parameter:string -> Format_code.t -> t
