(* BSD 3-Clause License

   Copyright (c) 2020, Anurag Soni All rights reserved.

   Redistribution and use in source and binary forms, with or without modification, are
   permitted provided that the following conditions are met:

   1. Redistributions of source code must retain the above copyright notice, this list of
   conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright notice, this list
   of conditions and the following disclaimer in the documentation and/or other materials
   provided with the distribution.

   3. Neither the name of the copyright holder nor the names of its contributors may be
   used to endorse or promote products derived from this software without specific prior
   written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
   EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
   MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
   THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
   INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
   STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
   THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. *)

open Postgres

type error =
  [ `Exn of exn
  | `Msg of string
  ]

let ( >>|? ) = Lwt_result.( >|= )

let wakeup_exn p w err =
  let res =
    match err with
    | `Exn _ as e -> e
    | `Msg _ as e -> e
    | `Parse_error m -> `Msg m
    | `Postgres_error e ->
      let msg =
        Format.asprintf "%a" Sexplib0.Sexp.pp_hum (Backend.Error_response.sexp_of_t e)
      in
      `Msg msg
  in
  if Lwt.is_sleeping p then Lwt.wakeup_later w (Error res) else ()
;;

let wakeup_if_empty p w r = if Lwt.is_sleeping p then Lwt.wakeup_later w r else ()

let connect run user_info =
  let finished, wakeup = Lwt.wait () in
  let conn =
    Connection.connect user_info (wakeup_exn finished wakeup) (fun () ->
        wakeup_if_empty finished wakeup (Ok ()))
  in
  run conn;
  finished >>|? fun () -> conn
;;

let prepare ~statement ?(name = "") ?(oids = [||]) conn =
  let finished, wakeup = Lwt.wait () in
  Connection.prepare conn ~statement ~name ~oids (wakeup_exn finished wakeup) (fun () ->
      wakeup_if_empty finished wakeup (Ok ()));
  finished
;;

let execute ?(name = "") ?(statement = "") ?(parameters = [||]) on_data_row conn =
  let finished, wakeup = Lwt.wait () in
  Connection.execute
    conn
    ~name
    ~statement
    ~parameters
    on_data_row
    (wakeup_exn finished wakeup)
    (fun () -> wakeup_if_empty finished wakeup (Ok ()));
  finished
;;

let close conn =
  Connection.close conn;
  Lwt_result.return ()
;;
