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

open Import
open Types
module Serializer = Frontend.Private.Serializer
module Parser = Backend.Private.Parser

exception Auth_method_not_implemented of string

module User_info = struct
  type t =
    { user : string
    ; password : string
    ; database : string option
    }

  let make ~user ?(password = "") ?database () = { user; password; database }
end

type error_handler = Error.t -> unit

type mode =
  | Connect
  | Parse
  | Execute
  | Close_statement_portal

type t =
  { user_info : User_info.t
  ; backend_key_data : Backend.Backend_key_data.t option
  ; reader : Parser.t
  ; writer : Serializer.t
  ; mutable on_error : error_handler
  ; mutable state : mode
  ; mutable ready_for_query : unit -> unit
  ; mutable on_data_row : string option list -> unit
  ; mutable running_operation : bool
  }

module Runtime = struct
  type nonrec t = t

  let next_write_operation t = Serializer.next_operation t.writer
  let next_read_operation t = Parser.next_action t.reader

  let read t buf ~off ~len =
    Parser.feed t.reader ~buf ~off ~len Angstrom.Unbuffered.Incomplete
  ;;

  let read_eof t buf ~off ~len =
    Parser.feed t.reader ~buf ~off ~len Angstrom.Unbuffered.Complete
  ;;

  let yield_reader _t thunk = thunk ()
  let yield_writer t thunk = Serializer.yield_writer t.writer thunk
  let report_write_result t res = Serializer.report_write_result t.writer res

  let shutdown t =
    Parser.force_close t.reader;
    Serializer.close_and_drain t.writer;
    Serializer.wakeup_writer t.writer
  ;;

  let report_exn t exn =
    shutdown t;
    t.on_error (Error.of_exn exn)
  ;;
end

type driver = (module Runtime_intf.S with type t = t) -> t -> unit

let handle_auth_message t msg =
  let r msg = raise @@ Auth_method_not_implemented msg in
  match msg with
  | Backend.Auth.Ok -> ()
  | Md5Password salt ->
    let hash =
      Auth.Md5.hash ~username:t.user_info.user ~password:t.user_info.password ~salt
    in
    let password_message = Frontend.Password_message.Md5_or_plain hash in
    Serializer.password t.writer password_message;
    Serializer.wakeup_writer t.writer
  | KerberosV5 -> r "kerberos"
  | CleartextPassword ->
    let password_message = Frontend.Password_message.Md5_or_plain t.user_info.password in
    Serializer.password t.writer password_message;
    Serializer.wakeup_writer t.writer
  | SCMCredential -> r "SCMCredential"
  | GSS -> r "GSS"
  | SSPI -> r "SSPI"
  | GSSContinue _ -> r "GSSContinue"
  | SASL _ -> r "SASL"
  | SASLContinue _ -> r "SASLContinue"
  | SASLFinal _ -> r "SASLFinal"
;;

let handle_connect t msg =
  match msg with
  | Backend.Auth msg -> handle_auth_message t msg
  | ParameterStatus s ->
    Logger.debug (fun m ->
        m "ParameterStatus: (%S, %S)" s.Backend.Parameter_status.name s.value)
  | NoticeResponse msg ->
    Logger.warn (fun m ->
        m
          "PostgresWarning: %a"
          Sexplib0.Sexp.pp_hum
          (Backend.Notice_response.sexp_of_t msg))
  | ReadyForQuery _ ->
    t.ready_for_query ();
    t.running_operation <- false;
    t.ready_for_query <- (fun () -> ())
  | _ ->
    (* Ignore other messages during startup *)
    ()
;;

let handle_parse t msg =
  match msg with
  | Backend.ParseComplete -> ()
  | ReadyForQuery _ ->
    t.ready_for_query ();
    t.running_operation <- false;
    t.ready_for_query <- (fun () -> ())
  | _ -> ()
;;

let handle_execute t msg =
  match msg with
  | Backend.BindComplete -> ()
  | ReadyForQuery _ ->
    t.ready_for_query ();
    t.running_operation <- false;
    t.ready_for_query <- (fun () -> ());
    t.on_data_row <- (fun _ -> ())
  | DataRow row ->
    (match t.on_data_row row with
    | () -> ()
    | exception exn ->
      Logger.debug (fun m ->
          m
            "Exception raised in the user provided row handler, discarding all future \
             data-rows.");
      t.on_data_row <- (fun _ -> ());
      t.ready_for_query <- (fun () -> t.on_error (Error.of_exn exn)))
  | _ -> ()
;;

let handle_close t msg =
  match msg with
  | Backend.CloseComplete -> t.ready_for_query ()
  | _ -> ()
;;

let handle_message' t msg =
  match msg with
  | Backend.ErrorResponse e ->
    t.on_error (Error.of_sexp (Backend.Error_response.sexp_of_t e))
  | _ ->
    (match t.state with
    | Connect -> handle_connect t msg
    | Parse -> handle_parse t msg
    | Execute -> handle_execute t msg
    | Close_statement_portal -> handle_close t msg)
;;

let startup driver user_info on_error finish =
  let rec handle_message msg =
    let t = Lazy.force t in
    handle_message' t msg
  and on_finish () =
    let t = Lazy.force t in
    finish t
  and t =
    lazy
      { user_info
      ; on_error
      ; backend_key_data = None
      ; reader = Parser.create Backend.parse handle_message
      ; writer = Serializer.create ()
      ; state = Connect
      ; ready_for_query = on_finish
      ; on_data_row = (fun _ -> ())
      ; running_operation = false
      }
  in
  let t' = Lazy.force t in
  driver (module Runtime : Runtime_intf.S with type t = t) t';
  Logger.debug (fun m -> m "startup");
  let startup_message =
    Frontend.Startup_message.make ~user:user_info.user ?database:user_info.database ()
  in
  Serializer.startup t'.writer startup_message;
  Serializer.wakeup_writer t'.writer
;;

let prepare conn ~statement ?(name = "") ?(oids = [||]) on_error finish =
  Logger.debug (fun m -> m "prepare");
  conn.ready_for_query <- finish;
  conn.on_error <- on_error;
  conn.state <- Parse;
  let prepare =
    { Frontend.Parse.name = Optional_string.of_string name; statement; oids }
  in
  Serializer.parse conn.writer prepare;
  Serializer.sync conn.writer;
  Serializer.wakeup_writer conn.writer
;;

let execute
    conn
    ?(portal_name = "")
    ?(statement_name = "")
    ?(parameters = [||])
    on_data_row
    on_error
    finish
  =
  conn.ready_for_query <- finish;
  conn.state <- Execute;
  conn.on_error <- on_error;
  conn.on_data_row <- on_data_row;
  let parameters =
    Array.map
      (fun (format_code, parameter) -> Frontend.Bind.make_param format_code ?parameter ())
      parameters
  in
  let b =
    Frontend.Bind.make ~destination:portal_name ~statement:statement_name ~parameters ()
  in
  let e = Frontend.Execute.make ~name:portal_name `Unlimited () in
  Serializer.bind conn.writer b;
  Serializer.execute conn.writer e;
  Serializer.sync conn.writer;
  Serializer.wakeup_writer conn.writer
;;

let close msg conn on_error finish =
  conn.ready_for_query <- finish;
  conn.on_error <- on_error;
  conn.state <- Close_statement_portal;
  Serializer.close conn.writer msg;
  Serializer.flush conn.writer;
  Serializer.wakeup_writer conn.writer
;;

let terminate conn =
  Serializer.terminate conn.writer;
  Runtime.shutdown conn
;;

module type IO = sig
  type 'a t

  val return : 'a -> 'a t
  val ( >>= ) : 'a t -> ('a -> 'b t) -> 'b t
  val of_cps : (error_handler -> ('a -> unit) -> unit) -> 'a t

  module Sequencer : sig
    type 'a future = 'a t
    type 'a t

    val create : 'a -> 'a t
    val enqueue : 'a t -> ('a -> 'b future) -> 'b future
  end
end

module type S = sig
  type 'a future
  type t

  val startup : driver -> User_info.t -> t future

  val prepare
    :  ?name:string
    -> ?oids:Types.Oid.t array
    -> statement:string
    -> t
    -> unit future

  val execute
    :  ?portal_name:string
    -> ?statement_name:string
    -> ?parameters:(Types.Format_code.t * string option) array
    -> (string option list -> unit)
    -> t
    -> unit future

  val close : Frontend.Close.t -> t -> unit future
  val terminate : t -> unit future
end

module Make (Io : IO) = struct
  open Io

  type t = Runtime.t Sequencer.t

  let ( let* ) = ( >>= )

  let startup driver user_info =
    let* conn = of_cps (startup driver user_info) in
    return (Sequencer.create conn)
  ;;

  let prepare ?name ?oids ~statement t =
    Sequencer.enqueue t @@ fun conn -> of_cps (prepare conn ~statement ?name ?oids)
  ;;

  let execute ?portal_name ?statement_name ?parameters on_data_row t =
    Sequencer.enqueue t
    @@ fun conn ->
    of_cps (execute conn ?portal_name ?statement_name ?parameters on_data_row)
  ;;

  let close msg t = Sequencer.enqueue t @@ fun conn -> of_cps (close msg conn)

  let terminate t =
    Sequencer.enqueue t (fun conn ->
        terminate conn;
        return ())
  ;;
end
