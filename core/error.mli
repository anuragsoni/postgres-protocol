type t

val of_string : string -> t
val of_sexp : Sexplib0.Sexp.t -> t
val of_thunk : (unit -> Sexplib0.Sexp.t) -> t
val to_sexp : t -> Sexplib0.Sexp.t
