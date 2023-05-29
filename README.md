# Postgres-ocaml

Work-in-progress OCaml implementation of the Postgres frontend/backend protocol. This library implements the wire protocol in OCaml, instead of using C ffi with `libpq`.

Only postgresql [extended query](https://www.postgresql.org/docs/15/protocol-flow.html) workflow is supported. The library contains the following components:

## [postgres](./postgres/)

This is the core library that defines all backend/frontend types. This library performs no I/O and is intended as a building block for I/O wrappers using lwt, async, etc. The library also includes a bare bones implementation of [RFC 5802](https://datatracker.ietf.org/doc/html/rfc5802) that covers just enough of the spec to address the needs for using `scram-sha-256` auth mechanism with postgresql.

## [postgres_eio](./postgres_eio/)

[Eio](https://github.com/ocaml-multicore/eio) driver for postgresql.

### Example

```ocaml
open Core
open Eio.Std
open Or_error.Let_syntax

let make_parameters ids =
  Sequence.of_list ids
  |> Sequence.map ~f:(fun id ->
       let b = Bytes.create 4 in
       Caml.Bytes.set_int32_be b 0 id;
       Postgres.Param.create
         ~parameter:(Bytes.to_string b)
         Postgres.Format_code.binary)
  |> Sequence.to_array
;;

let run ~sw ~net ~addr ~user ~password ~database =
  let%bind conn = Postgres_eio.connect ~sw ~net ~user ~password ~database addr in
  traceln "Logged in to postgres";
  let%bind () =
    Postgres_eio.prepare conn "SELECT id, email from users where id IN ($1, $2, $3)"
  in
  let%bind () = Postgres_eio.execute ~on_data_row:ignore conn in
  let parameters = make_parameters [ 1l; 2l; 3l ] in
  Postgres_eio.execute conn ~parameters ~on_data_row:(fun data_row ->
    match data_row with
    | [| Some id; Some name |] -> traceln "Id: %s and email: %s" id name
    | _ -> assert false)
;;

let main ~env ~user ~password ~database =
  let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, 5432) in
  Mirage_crypto_rng_eio.run
    (module Mirage_crypto_rng.Fortuna)
    env
    (fun () ->
      Switch.run (fun sw ->
        Or_error.ok_exn
          (run ~user ~password ~addr ~database ~sw ~net:(Eio.Stdenv.net env))))
;;
```

## TODO

* add wrapper to handle parameters in an easier manner
* Support transactions
* Support postgres notify/listen channels
* documentation
* support TLS encrypted connections
* TEST!!
