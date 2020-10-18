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

let src = Logs.Src.create "postgres.protocol"

module Log = (val Logs.src_log src : Logs.LOG)

let write_cstr f s =
  Faraday.write_string f s;
  Faraday.write_uint8 f 0

let parse_cstr = Angstrom.(take_while (fun c -> c <> '\x00') <* char '\x00')

module Types = Types
open Types

module Auth = struct
  module Md5 = struct
    (* https://www.postgresql.org/docs/12/protocol-flow.html#id-1.10.5.7.3 *)
    let hash ~username ~password ~salt =
      let digest = Digest.string (password ^ username) in
      let hex = Digest.to_hex digest in
      let digest = Digest.string (hex ^ salt) in
      let hex = Digest.to_hex digest in
      Printf.sprintf "md5%s" hex
  end
end

type protocol_version = V3_0

let write_protocol_version f = function
  | V3_0 ->
    Faraday.BE.write_uint16 f 3;
    Faraday.BE.write_uint16 f 0

module Frontend = struct
  module Startup_message = struct
    let ident = None

    type t =
      { user : string
      ; database : string option
      ; protocol_version : protocol_version
      }

    let make ~user ?database () = { user; database; protocol_version = V3_0 }

    let write f { user; database; protocol_version } =
      write_protocol_version f protocol_version;
      write_cstr f "user";
      write_cstr f user;
      Option.iter
        (fun d ->
          write_cstr f "database";
          write_cstr f d)
        database;
      Faraday.write_uint8 f 0

    let size { user; database; _ } =
      let user_len = 4 + 1 + String.length user + 1 in
      let database_len =
        match database with
        | None -> 0
        | Some d -> 8 + 1 + String.length d + 1
      in
      4 + user_len + database_len + 1
  end

  module Password_message = struct
    let ident = Some 'p'

    type t = Md5_or_plain of string

    let size (Md5_or_plain s) = String.length s + 1
    let write f (Md5_or_plain s) = write_cstr f s
  end

  module Parse = struct
    let ident = Some 'P'

    type t =
      { name : Optional_string.t
      ; statement : string
      ; oids : Oid.t Array.t
      }

    let size { name; statement; oids } =
      Optional_string.length name
      + 1
      + String.length statement
      + 1
      + 2
      + (Array.length oids * 4)

    let write f { name; statement; oids } =
      write_cstr f (Optional_string.to_string name);
      write_cstr f statement;
      Faraday.BE.write_uint16 f (Array.length oids);
      Array.iter (fun oid -> Faraday.BE.write_uint32 f (Oid.to_int32 oid)) oids
  end

  module Bind = struct
    let ident = Some 'B'

    type parameter =
      { format_code : Format_code.t
      ; parameter : string option
      }

    let make_param format_code ?parameter () = { format_code; parameter }

    type t =
      { destination : Optional_string.t
      ; statement : Optional_string.t
      ; parameters : parameter Array.t
      ; result_formats : Format_code.t Array.t
      }

    let make
        ?(destination = "")
        ?(statement = "")
        ?(parameters = Array.of_list [])
        ?(result_formats = Array.of_list [])
        ()
      =
      { destination = Optional_string.of_string destination
      ; statement = Optional_string.of_string statement
      ; parameters
      ; result_formats
      }

    let size { destination; statement; parameters; result_formats } =
      let param_len = Array.length parameters in
      Optional_string.length destination
      + 1
      + Optional_string.length statement
      + 1
      + 2
      + (param_len * 2)
      + 2
      + Array.fold_left
          (fun acc { parameter; _ } ->
            acc + 4 + String.length (Option.value parameter ~default:""))
          0
          parameters
      + 2
      + (Array.length result_formats * 2)

    let write f { destination; statement; parameters; result_formats } =
      write_cstr f (Optional_string.to_string destination);
      write_cstr f (Optional_string.to_string statement);
      Faraday.BE.write_uint16 f (Array.length parameters);
      Array.iter
        (fun { format_code; _ } ->
          Faraday.BE.write_uint16 f (Format_code.to_int format_code))
        parameters;
      Faraday.BE.write_uint16 f (Array.length parameters);
      Array.iter
        (fun { parameter; _ } ->
          let len =
            match parameter with
            | None -> -1
            | Some v -> String.length v
          in
          Faraday.BE.write_uint32 f (Int32.of_int len);
          Option.iter (Faraday.write_string f) parameter)
        parameters;
      Faraday.BE.write_uint16 f (Array.length result_formats);
      Array.iter
        (fun fmt -> Faraday.BE.write_uint16 f (Format_code.to_int fmt))
        result_formats
  end

  module Execute = struct
    let ident = Some 'E'

    type t =
      { name : Optional_string.t
      ; max_rows : [ `Unlimited | `Count of Positive_int32.t ]
      }

    let make ?(name = "") max_rows () =
      { name = Optional_string.of_string name; max_rows }

    let size { name; _ } = Optional_string.length name + 1 + 4

    let write f { name; max_rows } =
      write_cstr f (Optional_string.to_string name);
      let count =
        match max_rows with
        | `Unlimited -> 0l
        | `Count p -> Positive_int32.to_int32 p
      in
      Faraday.BE.write_uint32 f count
  end

  let sync = 'S'
  let terminate = 'X'
end

module Backend = struct
  open Angstrom

  module Header = struct
    type t =
      { length : int
      ; kind : char
      }

    let pp =
      let l = Fmt.field "length" (fun t -> t.length) Fmt.int
      and k = Fmt.field "kind" (fun t -> t.kind) Fmt.char in
      Fmt.record [ l; k ]

    let pp_dump =
      let l = Fmt.Dump.field "length" (fun t -> t.length) Fmt.int
      and k = Fmt.Dump.field "kind" (fun t -> t.kind) Fmt.char in
      Fmt.Dump.record [ l; k ]

    let parse =
      let parse_len =
        BE.any_int32
        >>= fun l ->
        let l = Int32.to_int l in
        if l < 4 then fail (Printf.sprintf "Invalid payload length: %d" l) else return l
      in
      lift2 (fun kind length -> { kind; length }) any_char parse_len <?> "MESSAGE_HEADER"
  end

  module Auth = struct
    type t =
      | Ok
      | KerberosV5
      | CleartextPassword
      | Md5Password of string
      | SCMCredential
      | GSS
      | SSPI
      | GSSContinue of string
      | SASL of string
      | SASLContinue of string
      | SASLFinal of string

    let to_string = function
      | Ok -> "Auth_ok"
      | KerberosV5 -> "Auth_KerberosV5"
      | CleartextPassword -> "Auth_Cleartext"
      | Md5Password _ -> "Auth_Md5Password"
      | SCMCredential -> "Auth_SCMCredential"
      | GSS -> "Auth_Gss"
      | SSPI -> "Auth_Sspi"
      | GSSContinue _ -> "Auth_Gss_Continue"
      | SASL _ -> "Auth_Sasl"
      | SASLContinue _ -> "Auth_Sasl_Continue"
      | SASLFinal _ -> "Auth_Sasl_Final"

    let pp = Fmt.of_to_string to_string

    let parse { Header.length = len; _ } =
      let p =
        BE.any_int32
        >>= function
        | 0l -> return Ok <?> "AUTH_OK"
        | 2l -> return KerberosV5 <?> "AUTH_KERBEROSV5"
        | 3l -> return CleartextPassword <?> "AUTH_CLEARTEXT"
        | 5l -> lift (fun salt -> Md5Password salt) (take 4) <?> "AUTH_MD5"
        | 6l -> return SCMCredential <?> "AUTH_SCM_CREDENTIAL"
        | 7l -> return GSS <?> "AUTH_GSS"
        | 9l -> return SSPI <?> "AUTH_SSPI"
        | 8l -> lift (fun s -> GSSContinue s) (take (len - 4 - 4)) <?> "AUTH_GSS_CONTINUE"
        | 10l -> lift (fun s -> SASL s) (take (len - 4 - 4)) <?> "AUTH_SASL"
        | 11l ->
          lift (fun s -> SASLContinue s) (take (len - 4 - 4)) <?> "AUTH_SASL_CONTINUE"
        | 12l -> lift (fun s -> SASLFinal s) (take (len - 4 - 4)) <?> "AUTH_SASL_FINAL"
        | k -> fail (Printf.sprintf "Unknown authentication type: %ld" k)
      in
      p <?> "PARSE_AUTH"
  end

  module Backend_key_data = struct
    type t =
      { pid : Process_id.t
      ; secret : Int32.t
      }

    let pp =
      let p = Fmt.field "pid" (fun t -> t.pid) Process_id.pp
      and s = Fmt.field "secret" (fun _ -> "<opaque>") Fmt.string in
      Fmt.record [ p; s ]

    let pp_dump =
      let p = Fmt.Dump.field "pid" (fun t -> t.pid |> Process_id.to_int32) Fmt.int32
      and s = Fmt.Dump.field "secret" (fun _ -> "<opaque>") Fmt.string in
      Fmt.Dump.record [ p; s ]

    let parse _header =
      let parse_pid =
        BE.any_int32
        >>= (fun pid ->
              match Process_id.of_int32 pid with
              | None -> fail @@ Printf.sprintf "Invalid process id: %ld" pid
              | Some v -> return v)
        <?> "PROCESS_ID"
      in
      lift2 (fun pid secret -> { pid; secret }) parse_pid BE.any_int32
      <?> "BACKEND_KEY_DATA"
  end

  module Error_or_notice_kind = struct
    type t =
      | Severity
      | Non_localized_severity
      | Code
      | Message
      | Detail
      | Hint
      | Position
      | Internal_position
      | Internal_query
      | Where
      | Schema_name
      | Table_name
      | Column_name
      | Datatype_name
      | Constraint_name
      | File
      | Line
      | Routine
      | Unknown of char

    let to_string = function
      | Severity -> "Severity"
      | Non_localized_severity -> "Non localized severity"
      | Code -> "Code"
      | Message -> "Message"
      | Detail -> "Detail"
      | Hint -> "Hint"
      | Position -> "Position"
      | Internal_position -> "Internal Position"
      | Internal_query -> "Internal Query"
      | Where -> "Where"
      | Schema_name -> "Schema Name"
      | Table_name -> "Table Name"
      | Column_name -> "Column Name"
      | Datatype_name -> "Datatype Name"
      | Constraint_name -> "Constraint Name"
      | File -> "File"
      | Line -> "Line"
      | Routine -> "Routine"
      | Unknown c -> Printf.sprintf "Unknown code: %C" c

    let pp = Fmt.of_to_string to_string

    let parse =
      any_char
      >>= fun c ->
      let t =
        match c with
        | 'S' -> Severity
        | 'V' -> Non_localized_severity
        | 'C' -> Code
        | 'M' -> Message
        | 'D' -> Detail
        | 'H' -> Hint
        | 'P' -> Position
        | 'p' -> Internal_position
        | 'q' -> Internal_query
        | 'W' -> Where
        | 's' -> Schema_name
        | 't' -> Table_name
        | 'c' -> Column_name
        | 'd' -> Datatype_name
        | 'n' -> Constraint_name
        | 'F' -> File
        | 'L' -> Line
        | 'R' -> Routine
        | c -> Unknown c
      in
      return t
  end

  module Make_error_notice (K : sig
    val label : string
  end) =
  struct
    type t =
      { code : Error_or_notice_kind.t
      ; message : Optional_string.t
      }

    let pp =
      let c = Fmt.field "code" (fun t -> t.code) Error_or_notice_kind.pp
      and m = Fmt.field "message" (fun t -> t.message) Optional_string.pp in
      Fmt.record [ c; m ]

    let pp_dump =
      let c = Fmt.Dump.field "code" (fun t -> t.code) Error_or_notice_kind.pp
      and m = Fmt.Dump.field "message" (fun t -> t.message) Optional_string.pp in
      Fmt.Dump.record [ c; m ]

    let parse_message =
      lift Optional_string.of_string parse_cstr <?> Printf.sprintf "%s_MESSAGE" K.label

    let parse _header =
      lift2
        (fun code message -> { code; message })
        Error_or_notice_kind.parse
        parse_message
      <?> K.label
  end

  module Error_response = Make_error_notice (struct
    let label = "ERROR_RESPONSE"
  end)

  module Notice_response = Make_error_notice (struct
    let label = "NOTICE_RESPONSE"
  end)

  module Parameter_status = struct
    type t =
      { name : string
      ; value : string
      }

    let pp =
      let n = Fmt.field "name" (fun t -> t.name) Fmt.string
      and v = Fmt.field "value" (fun t -> t.value) Fmt.string in
      Fmt.record [ n; v ]

    let pp_dump =
      let n = Fmt.Dump.field "name" (fun t -> t.name) Fmt.string
      and v = Fmt.Dump.field "value" (fun t -> t.value) Fmt.string in
      Fmt.record [ n; v ]

    let parse _header =
      lift2 (fun name value -> { name; value }) parse_cstr parse_cstr
      <?> "PARAMETER_STATUS"
  end

  module Ready_for_query = struct
    type t =
      | Idle
      | Transaction_block
      | Failed_transaction

    let to_string = function
      | Idle -> "Idle"
      | Transaction_block -> "Transaction_block"
      | Failed_transaction -> "Failed_transaction"

    let pp = Fmt.of_to_string to_string

    let parse _header =
      any_char
      >>= (function
            | 'I' -> return Idle
            | 'T' -> return Transaction_block
            | 'E' -> return Failed_transaction
            | c -> fail @@ Printf.sprintf "Unknown response for ReadyForQuery: %C" c)
      <?> "READY_FOR_QUERY"
  end

  module Data_row = struct
    let parse _header =
      BE.any_int16
      >>= fun len ->
      count
        len
        (BE.any_int32
        >>= fun l ->
        if l = -1l then return None else lift Option.some @@ take (Int32.to_int l))
  end

  type message =
    | Auth of Auth.t
    | BackendKeyData of Backend_key_data.t
    | ErrorResponse of Error_response.t
    | NoticeResponse of Notice_response.t
    | ParameterStatus of Parameter_status.t
    | ReadyForQuery of Ready_for_query.t
    | ParseComplete
    | BindComplete
    | DataRow of string option list
    | UnknownMessage of char

  let parse =
    Header.parse
    <* commit
    >>= fun ({ kind; length } as header) ->
    Log.debug (fun m -> m "Message received of kind: %C" kind);
    match kind with
    | 'R' -> lift (fun m -> Auth m) @@ Auth.parse header
    | 'K' -> lift (fun m -> BackendKeyData m) @@ Backend_key_data.parse header
    | 'E' -> lift (fun m -> ErrorResponse m) @@ Error_response.parse header
    | 'N' -> lift (fun m -> NoticeResponse m) @@ Notice_response.parse header
    | 'S' -> lift (fun m -> ParameterStatus m) @@ Parameter_status.parse header
    | 'Z' -> lift (fun m -> ReadyForQuery m) @@ Ready_for_query.parse header
    | '1' -> return ParseComplete
    | '2' -> return BindComplete
    | 'D' -> lift (fun m -> DataRow m) @@ Data_row.parse header
    | c ->
      Log.warn (fun m -> m "Received an unknown message with ident: %C" c);
      take (length - 4) *> (return @@ UnknownMessage c)
end

module Serializer = struct
  type t =
    { writer : Faraday.t
    ; mutable wakeup_writer : (unit -> unit) option
    ; mutable drained_bytes : int
    }

  let create ?(size = 0x1000) () =
    { writer = Faraday.create size; wakeup_writer = None; drained_bytes = 0 }

  let yield_writer t thunk =
    if Faraday.is_closed t.writer then failwith "Serializer is closed";
    match t.wakeup_writer with
    | None -> t.wakeup_writer <- Some thunk
    | Some _ -> failwith "Only one write callback can be registered at a time"

  let wakeup_writer t =
    let thunk = t.wakeup_writer in
    t.wakeup_writer <- None;
    Option.iter (fun t -> t ()) thunk

  let is_closed t = Faraday.is_closed t.writer
  let drain t = Faraday.drain t.writer

  let close_and_drain t =
    Faraday.close t.writer;
    t.drained_bytes <- drain t

  let report_write_result t res =
    match res with
    | `Closed -> close_and_drain t
    | `Ok n -> Faraday.shift t.writer n

  module type Message = sig
    type t

    val ident : char option
    val size : t -> int
    val write : Faraday.t -> t -> unit
  end

  let write (type a) (module M : Message with type t = a) msg t =
    let header_length = 4 in
    Option.iter (fun c -> Faraday.write_char t.writer c) M.ident;
    Faraday.BE.write_uint32 t.writer (Int32.of_int @@ (M.size msg + header_length));
    M.write t.writer msg

  let write_ident_only ident t =
    Faraday.write_char t.writer ident;
    Faraday.BE.write_uint32 t.writer 4l

  let startup t msg = write (module Frontend.Startup_message) msg t
  let password t msg = write (module Frontend.Password_message) msg t
  let parse t msg = write (module Frontend.Parse) msg t
  let bind t msg = write (module Frontend.Bind) msg t
  let execute t msg = write (module Frontend.Execute) msg t
  let sync t = write_ident_only Frontend.sync t
  let terminate t = write_ident_only Frontend.terminate t

  let next_operation t =
    match Faraday.operation t.writer with
    | `Close -> `Close t.drained_bytes
    | `Yield -> `Yield
    | `Writev iovecs -> `Write iovecs
end

module Parser = struct
  module U = Angstrom.Unbuffered

  type t =
    { mutable parse_state : unit U.state
    ; parser : unit Angstrom.t
    ; mutable closed : bool
    }

  let create parser handler =
    let parser = Angstrom.(skip_many (parser <* commit >>| handler)) in
    { parser; parse_state = U.Done (0, ()); closed = false }

  let next_action t =
    match t.parse_state with
    | _ when t.closed -> `Close
    | U.Done _ -> `Read
    | Partial _ -> `Read
    | Fail (_, _, _msg) -> `Close

  let parse t ~buf ~off ~len more =
    let rec aux t =
      match t.parse_state with
      | U.Partial { continue; _ } -> t.parse_state <- continue buf ~off ~len more
      | U.Done (0, ()) ->
        t.parse_state <- U.parse t.parser;
        aux t
      | U.Done _ -> t.parse_state <- U.Done (0, ())
      | U.Fail _ -> ()
    in
    aux t;
    match t.parse_state with
    | U.Partial { committed; _ } | U.Done (committed, ()) | U.Fail (committed, _, _) ->
      committed

  let feed t ~buf ~off ~len more =
    let committed = parse t ~buf ~off ~len more in
    (match more with
    | U.Complete -> t.closed <- true
    | Incomplete -> ());
    committed

  let is_closed t = t.closed
  let force_close t = t.closed <- true
end

module Request_ssl = struct
  let to_response = function
    | 'S' -> `Available
    | _ -> `Unavailable

  let payload =
    let b = Bytes.create 8 in
    Bytes.set_int32_be b 0 8l;
    Bytes.set_int16_be b 4 1234;
    Bytes.set_int16_be b 6 5679;
    b

  type state =
    | Write
    | Read
    | Fail of string
    | Closed

  type t =
    { mutable state : state
    ; on_finish : [ `Available | `Unavailable ] -> unit
    }

  let create on_finish = { state = Write; on_finish }

  let next_operation t =
    match t.state with
    | Write -> `Write payload
    | Read -> `Read
    | Fail msg -> `Fail msg
    | Closed -> `Stop

  let report_write_result t = function
    | 8 -> t.state <- Read
    | _ -> t.state <- Fail "Could not write the ssl request payload successfully."

  let feed_char t c =
    let resp = to_response c in
    t.state <- Closed;
    t.on_finish resp
end

module Connection = struct
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

  let is_conn_closed conn =
    Parser.is_closed conn.reader && Serializer.is_closed conn.writer

  module Sequencer = struct
    type t =
      { conn : conn
      ; queue : (conn -> unit) Queue.t
      }

    let conn t = t.conn
    let create conn = { conn; queue = Queue.create () }

    let enqueue t on_error task =
      Log.debug (fun m -> m "sequencer enqueue");
      if is_conn_closed t.conn
      then on_error (`Msg "Connection already closed")
      else (
        let run conn =
          conn.on_error <- on_error;
          task conn
        in
        Queue.push run t.queue)

    let is_empty t = Queue.is_empty t.queue

    let advance_if_needed t =
      match Queue.peek_opt t.queue with
      | None ->
        Log.debug (fun m -> m "no advance");
        ()
      | Some _ ->
        Log.debug (fun m -> m "advance");
        if t.conn.running_operation
        then ()
        else (
          let op = Queue.take t.queue in
          t.conn.running_operation <- true;
          op t.conn)
  end

  type t = Sequencer.t

  let next_write_operation t =
    Sequencer.advance_if_needed t;
    Serializer.next_operation (Sequencer.conn t).writer

  let next_read_operation t =
    Sequencer.advance_if_needed t;
    if Sequencer.is_empty t && (Sequencer.conn t).running_operation = false
    then `Yield
    else Parser.next_action (Sequencer.conn t).reader

  let read t buf ~off ~len =
    Parser.feed (Sequencer.conn t).reader ~buf ~off ~len Angstrom.Unbuffered.Incomplete

  let read_eof t buf ~off ~len =
    Parser.feed (Sequencer.conn t).reader ~buf ~off ~len Angstrom.Unbuffered.Complete

  let yield_reader t thunk =
    if is_conn_closed (Sequencer.conn t)
    then failwith "Connection is closed"
    else if Option.is_some (Sequencer.conn t).wakeup_reader
    then failwith "Only one callback can be registered at a time"
    else (Sequencer.conn t).wakeup_reader <- Some thunk

  let wakeup_reader t =
    let conn = Sequencer.conn t in
    let thunk = conn.wakeup_reader in
    conn.wakeup_reader <- None;
    Option.iter (fun t -> t ()) thunk

  let yield_writer t thunk = Serializer.yield_writer (Sequencer.conn t).writer thunk

  let report_write_result t res =
    Serializer.report_write_result (Sequencer.conn t).writer res

  let shutdown conn =
    let t = Sequencer.conn conn in
    Parser.force_close t.reader;
    Serializer.close_and_drain t.writer;
    Serializer.wakeup_writer t.writer

  let report_exn t exn =
    shutdown t;
    (Sequencer.conn t).on_error (`Exn exn)

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
      let password_message =
        Frontend.Password_message.Md5_or_plain t.user_info.password
      in
      Serializer.password t.writer password_message;
      Serializer.wakeup_writer t.writer
    | SCMCredential -> r "SCMCredential"
    | GSS -> r "GSS"
    | SSPI -> r "SSPI"
    | GSSContinue _ -> r "GSSContinue"
    | SASL _ -> r "SASL"
    | SASLContinue _ -> r "SASLContinue"
    | SASLFinal _ -> r "SASLFinal"

  let handle_connect t msg =
    match msg with
    | Backend.Auth msg -> handle_auth_message t msg
    | ParameterStatus s ->
      Log.debug (fun m ->
          m "ParameterStatus: (%S, %S)" s.Backend.Parameter_status.name s.value)
    | NoticeResponse msg ->
      Log.warn (fun m -> m "PostgresWarning: %a" Backend.Notice_response.pp msg)
    | ReadyForQuery _ ->
      t.ready_for_query ();
      t.running_operation <- false;
      t.ready_for_query <- (fun () -> ())
    | _ ->
      (* Ignore other messages during startup *)
      ()

  let handle_parse t msg =
    match msg with
    | Backend.ParseComplete -> ()
    | ReadyForQuery _ ->
      t.ready_for_query ();
      t.running_operation <- false;
      t.ready_for_query <- (fun () -> ())
    | _ -> ()

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
        Log.debug (fun m ->
            m
              "Exception raised in the user provided row handler, discarding all future \
               data-rows.");
        t.on_data_row <- (fun _ -> ());
        t.ready_for_query <- (fun () -> t.on_error (`Exn exn)))
    | _ -> ()

  let handle_message' t msg =
    match msg with
    | Backend.ErrorResponse e ->
      Log.err (fun m -> m "Error: %a" Backend.Error_response.pp_dump e);
      t.on_error (`Postgres_error e)
    | _ ->
      (match t.state with
      | Connect -> handle_connect t msg
      | Parse -> handle_parse t msg
      | Execute -> handle_execute t msg)

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
    Log.debug (fun m -> m "startup");
    let startup_message =
      Frontend.Startup_message.make ~user:user_info.user ?database:user_info.database ()
    in
    Serializer.startup t.writer startup_message;
    Serializer.wakeup_writer t.writer);
    wakeup_reader conn;
    conn

  let prepare t ~statement ?(name = "") ?(oids = [||]) on_error finish =
    (Sequencer.enqueue t on_error
    @@ fun conn ->
    Log.debug (fun m -> m "prepare");
    conn.ready_for_query <- finish;
    conn.state <- Parse;
    let prepare =
      { Frontend.Parse.name = Optional_string.of_string name; statement; oids }
    in
    Serializer.parse conn.writer prepare;
    Serializer.sync conn.writer;
    Serializer.wakeup_writer conn.writer);
    wakeup_reader t

  let execute
      t
      ?(name = "")
      ?(statement = "")
      ?(parameters = [||])
      on_error
      on_data_row
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

  let close t =
    (Sequencer.enqueue t (fun _ -> ())
    @@ fun conn ->
    Serializer.terminate conn.writer;
    shutdown t);
    wakeup_reader t
end
