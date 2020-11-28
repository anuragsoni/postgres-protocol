type t = Sexplib0.Sexp.t Lazy.t

let of_string x = Lazy.from_val (Sexplib0.Sexp_conv.sexp_of_string x)
let of_sexp x = Lazy.from_val x
let of_thunk thunk = Lazy.from_fun thunk
let to_sexp t = Lazy.force t
