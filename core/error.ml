(* Error type based on Janestreet's Base.Error.t
   https://github.com/janestreet/base/blob/25e68e4a33c8339d95cb7e89485410d9be7f8771/src/info.ml

   The MIT License

   Copyright (c) 2016--2020 Jane Street Group, LLC <opensource@janestreet.com>

   Permission is hereby granted, free of charge, to any person obtaining a copy of this
   software and associated documentation files (the "Software"), to deal in the Software
   without restriction, including without limitation the rights to use, copy, modify,
   merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to the following
   conditions:

   The above copyright notice and this permission notice shall be included in all copies
   or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
   INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
   PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
   HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
   CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
   THE USE OR OTHER DEALINGS IN THE SOFTWARE. *)
open Sexplib0.Sexp_conv

module Kind = struct
  type t =
    | Exn of exn
    | Msg of string
    | Sexp of Sexplib0.Sexp.t
  [@@deriving sexp_of]
end

type t = Kind.t Lazy.t

exception Exn of Kind.t

(* Register a pretty printer for exceptions raised via Error module. Without this the user
   would see [Fatal error: exception Error.Exn(_)]. With the printer we will instead see a
   sexp formatted message. *)
let () =
  Printexc.register_printer (function
      | Exn t -> Some (Sexplib0.Sexp.to_string_hum (Kind.sexp_of_t t))
      | _ -> None)
;;

let of_exn exn = Lazy.from_val (Kind.Exn exn)
let of_string msg = Lazy.from_val (Kind.Msg msg)
let of_sexp s = Lazy.from_val (Kind.Sexp s)
let failf fmt = Format.kasprintf (fun v -> Error (of_string v)) fmt

let sexp_of_t t =
  let kind = Lazy.force t in
  Kind.sexp_of_t kind
;;

let to_exn t =
  let kind = Lazy.force t in
  match kind with
  | Kind.Exn exn -> raise exn
  | t -> raise (Exn t)
;;

let raise t = raise (to_exn t)
