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

open Types

exception Auth_method_not_implemented of string

module User_info = struct
  type t =
    { user : string
    ; password : string
    ; database : string option
    }

  let make ~user ?(password = "") ?database () = { user; password; database }
end

type error =
  [ `Exn of exn
  | `Msg of string
  | `Postgres_error of Backend.Error_response.t
  | `Parse_error of string
  ]

type error_handler = error -> unit

type mode =
  | Connect
  | Parse
  | Execute

type conn =
  { user_info : User_info.t
  ; backend_key_data : Backend.Backend_key_data.t option
  ; reader : Parser.t
  ; writer : Serializer.t
  ; mutable wakeup_reader : (unit -> unit) option
  ; mutable on_error : error_handler
  ; mutable state : mode
  ; mutable ready_for_query : unit -> unit
  ; mutable on_data_row : string option list -> unit
  ; mutable running_operation : bool
  }

let is_conn_closed conn = Parser.is_closed conn.reader && Serializer.is_closed conn.writer

module Sequencer = struct
  type job =
    { run : conn -> unit
    ; cancel : error -> unit
    }

  type t =
    { conn : conn
    ; queue : job Queue.t
    }

  let conn t = t.conn
  let create conn = { conn; queue = Queue.create () }

  let enqueue t on_error run =
    Logger.debug (fun m -> m "sequencer enqueue");
    if is_conn_closed t.conn
    then on_error (`Msg "Connection closed")
    else
      Queue.push
        { run =
            (fun conn ->
              conn.on_error <- on_error;
              run conn)
        ; cancel = on_error
        }
        t.queue
  ;;

  let shutdown t =
    Queue.iter (fun { cancel; _ } -> cancel (`Msg "Connection closed")) t.queue;
    Queue.clear t.queue
  ;;

  let is_empty t = Queue.is_empty t.queue

  let advance_if_needed t =
    match Queue.peek_opt t.queue with
    | None ->
      Logger.debug (fun m -> m "no advance");
      ()
    | Some _ ->
      Logger.debug (fun m -> m "advance");
      if t.conn.running_operation
      then ()
      else (
        let op = Queue.take t.queue in
        t.conn.running_operation <- true;
        op.run t.conn)
  ;;
end

type t = Sequencer.t

let next_write_operation t =
  Sequencer.advance_if_needed t;
  Serializer.next_operation (Sequencer.conn t).writer
;;

let next_read_operation t =
  Sequencer.advance_if_needed t;
  if Sequencer.is_empty t && (Sequencer.conn t).running_operation = false
  then `Yield
  else Parser.next_action (Sequencer.conn t).reader
;;

let read t buf ~off ~len =
  Parser.feed (Sequencer.conn t).reader ~buf ~off ~len Angstrom.Unbuffered.Incomplete
;;

let read_eof t buf ~off ~len =
  Parser.feed (Sequencer.conn t).reader ~buf ~off ~len Angstrom.Unbuffered.Complete
;;

let yield_reader t thunk =
  if is_conn_closed (Sequencer.conn t)
  then failwith "Connection is closed"
  else if Option.is_some (Sequencer.conn t).wakeup_reader
  then failwith "Only one callback can be registered at a time"
  else (Sequencer.conn t).wakeup_reader <- Some thunk
;;

let wakeup_reader t =
  let conn = Sequencer.conn t in
  let thunk = conn.wakeup_reader in
  conn.wakeup_reader <- None;
  Option.iter (fun t -> t ()) thunk
;;

let yield_writer t thunk = Serializer.yield_writer (Sequencer.conn t).writer thunk

let report_write_result t res =
  Serializer.report_write_result (Sequencer.conn t).writer res
;;

let shutdown conn =
  let t = Sequencer.conn conn in
  Sequencer.shutdown conn;
  Parser.force_close t.reader;
  Serializer.close_and_drain t.writer;
  Serializer.wakeup_writer t.writer
;;

let report_exn t exn =
  shutdown t;
  (Sequencer.conn t).on_error (`Exn exn)
;;

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
      t.ready_for_query <- (fun () -> t.on_error (`Exn exn)))
  | _ -> ()
;;

let handle_message' t msg =
  match msg with
  | Backend.ErrorResponse e -> t.on_error (`Postgres_error e)
  | _ ->
    (match t.state with
    | Connect -> handle_connect t msg
    | Parse -> handle_parse t msg
    | Execute -> handle_execute t msg)
;;

let connect user_info on_error finish =
  let rec handle_message msg =
    let t = Lazy.force t in
    handle_message' t msg
  and t =
    lazy
      { user_info
      ; on_error
      ; backend_key_data = None
      ; reader = Parser.create Backend.parse handle_message
      ; writer = Serializer.create ()
      ; state = Connect
      ; ready_for_query = finish
      ; on_data_row = (fun _ -> ())
      ; wakeup_reader = None
      ; running_operation = false
      }
  in
  let t' = Lazy.force t in
  let conn = Sequencer.create t' in
  (Sequencer.enqueue conn on_error
  @@ fun t ->
  Logger.debug (fun m -> m "startup");
  let startup_message =
    Frontend.Startup_message.make ~user:user_info.user ?database:user_info.database ()
  in
  Serializer.startup t.writer startup_message;
  Serializer.wakeup_writer t.writer);
  wakeup_reader conn;
  conn
;;

let prepare t ~statement ?(name = "") ?(oids = [||]) on_error finish =
  (Sequencer.enqueue t on_error
  @@ fun conn ->
  Logger.debug (fun m -> m "prepare");
  conn.ready_for_query <- finish;
  conn.state <- Parse;
  let prepare =
    { Frontend.Parse.name = Optional_string.of_string name; statement; oids }
  in
  Serializer.parse conn.writer prepare;
  Serializer.sync conn.writer;
  Serializer.wakeup_writer conn.writer);
  wakeup_reader t
;;

let execute
    t
    ?(name = "")
    ?(statement = "")
    ?(parameters = [||])
    on_data_row
    on_error
    finish
  =
  (Sequencer.enqueue t on_error
  @@ fun conn ->
  conn.ready_for_query <- finish;
  conn.state <- Execute;
  conn.on_data_row <- on_data_row;
  let b = Frontend.Bind.make ~destination:name ~statement ~parameters () in
  let e = Frontend.Execute.make ~name `Unlimited () in
  Serializer.bind conn.writer b;
  Serializer.execute conn.writer e;
  Serializer.sync conn.writer;
  Serializer.wakeup_writer conn.writer);
  wakeup_reader t
;;

let close t =
  (Sequencer.enqueue t (fun _ -> ())
  @@ fun conn ->
  Serializer.terminate conn.writer;
  shutdown t);
  wakeup_reader t
;;
