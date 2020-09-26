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

module Types = struct
  module Process_id = struct
    type t = Int32.t

    let to_int32 t = t
    let of_int32 t = if t < 1l then None else Some t

    let of_int32_exn t =
      match of_int32 t with
      | None ->
        raise
        @@ Invalid_argument
             (Printf.sprintf "Process id needs to be a positive integer. Received: %ld" t)
      | Some v -> v
  end

  module Statement_or_portal = struct
    type t =
      | Statement
      | Portal

    let to_char = function
      | Statement -> 'S'
      | Portal -> 'P'

    let of_char = function
      | 'S' -> Statement
      | 'P' -> Portal
      | c ->
        raise
        @@ Invalid_argument
             (Printf.sprintf "Expected Statement('S') or Portal('P') but received '%c'" c)
  end

  module Positive_int32 = struct
    type t = Int32.t

    let of_int32_exn t =
      if t > 0l
      then t
      else
        raise
        @@ Invalid_argument
             (Printf.sprintf "Expected positive integer, but received %ld instead" t)

    let to_int32 t = t
  end

  module Optional_string = struct
    type t = string

    let empty = ""
    let of_string = Fun.id
    let to_string = Fun.id
    let is_empty t = t = ""
    let length = String.length
  end

  module Oid = struct
    type t = Int32.t

    let of_int32 = Fun.id
    let of_int_exn = Int32.of_int
    let to_int32 = Fun.id
  end

  module Format_code = struct
    type t =
      [ `Text
      | `Binary
      ]

    let of_int = function
      | 0 -> Some `Text
      | 1 -> Some `Binary
      | _ -> None

    let to_int = function
      | `Text -> 0
      | `Binary -> 1
  end
end

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

    type t = Md5 of string

    let size (Md5 s) = String.length s + 1
    let write f (Md5 s) = write_cstr f s
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

    type t =
      { destination : Optional_string.t
      ; statement : Optional_string.t
      ; parameters : parameter Array.t
      ; result_formats : Format_code.t Array.t
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

    let size { name; _ } = Optional_string.length name + 1 + 4

    let write f { name; max_rows } =
      write_cstr f name;
      let count =
        match max_rows with
        | `Unlimited -> 0l
        | `Count p -> Positive_int32.to_int32 p
      in
      Faraday.BE.write_uint32 f count
  end

  module Close = struct
    let ident = Some 'C'

    type t =
      { kind : Statement_or_portal.t
      ; name : Optional_string.t
      }

    let size { name; _ } = 1 + Optional_string.length name + 1

    let write f { kind; name } =
      Faraday.write_char f (Statement_or_portal.to_char kind);
      write_cstr f (Optional_string.to_string name)
  end

  module Describe = struct
    let ident = Some 'D'

    type t =
      { kind : Statement_or_portal.t
      ; name : Optional_string.t
      }

    let size { name; _ } = 1 + Optional_string.length name + 1

    let write f { kind; name } =
      Faraday.write_char f (Statement_or_portal.to_char kind);
      write_cstr f (Optional_string.to_string name)
  end

  module Copy_fail = struct
    let ident = Some 'f'

    type t = string

    let of_string t = t
    let size t = String.length t + 1
    let write f t = write_cstr f t
  end

  let flush = 'H'
  let sync = 'S'
  let terminate = 'X'
  let copy_done = 'c'
end

module Backend = struct
  open Angstrom

  module Header = struct
    type t =
      { length : int
      ; kind : char
      }

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

    let parse _header =
      lift2 (fun name value -> { name; value }) parse_cstr parse_cstr
      <?> "PARAMETER_STATUS"
  end

  module Ready_for_query = struct
    type t =
      | Idle
      | Transaction_block
      | Failed_transaction

    let parse _header =
      any_char
      >>= (function
            | 'I' -> return Idle
            | 'T' -> return Transaction_block
            | 'E' -> return Failed_transaction
            | c -> fail @@ Printf.sprintf "Unknown response for ReadyForQuery: %C" c)
      <?> "READY_FOR_QUERY"
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
  let close t msg = write (module Frontend.Close) msg t
  let describe t msg = write (module Frontend.Describe) msg t
  let copy_fail t msg = write (module Frontend.Copy_fail) msg t
  let flush t = write_ident_only Frontend.flush t
  let sync t = write_ident_only Frontend.sync t
  let terminate t = write_ident_only Frontend.terminate t
  let copy_done t = write_ident_only Frontend.copy_done t

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

  let create handler =
    let parser = Angstrom.(skip_many (Backend.parse <* commit >>| handler)) in
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

module Connection = struct
  exception Auth_method_not_implemented of string

  type error =
    [ `Exn of exn
    | `Postgres_error of Backend.Error_response.t
    | `Parse_error of string
    ]

  type error_handler = error -> unit

  type state =
    | Connect
    | Ready_for_query
    | Parse

  (* | Bind *)
  (* | Execute *)

  type t =
    { user : string
    ; password : string
    ; database : string option
    ; backend_key_data : Backend.Backend_key_data.t option
    ; error_handler : error_handler
    ; reader : Parser.t
    ; writer : Serializer.t
    ; mutable state : state
    ; mutable ready_for_query : unit -> unit
    }

  let handle_auth_message t msg =
    let r msg = raise @@ Auth_method_not_implemented msg in
    match msg with
    | Backend.Auth.Ok -> ()
    | Md5Password salt ->
      let hash = Auth.Md5.hash ~username:t.user ~password:t.password ~salt in
      let password_message = Frontend.Password_message.Md5 hash in
      Serializer.password t.writer password_message;
      Serializer.wakeup_writer t.writer
    | KerberosV5 -> r "kerberos"
    | CleartextPassword -> r "clear text password"
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
    | ReadyForQuery _ ->
      Log.debug (fun m -> m "Ready for query");
      t.state <- Ready_for_query;
      t.ready_for_query ()
    | ParameterStatus s ->
      Log.debug (fun m ->
          m "ParameterStatus: (%S, %S)" s.Backend.Parameter_status.name s.value)
    | NoticeResponse msg ->
      Log.warn (fun m -> m "PostgresWarning: %S" msg.Backend.Notice_response.message)
    | _ ->
      (* Ignore other messages during startup *)
      ()

  let handle_parse t msg =
    match msg with
    | Backend.ParseComplete -> ()
    | ReadyForQuery _ -> t.ready_for_query ()
    | _ -> ()

  let handle_message' t msg =
    match msg with
    | Backend.ErrorResponse e ->
      Log.err (fun m -> m "Error: %S" e.Backend.Error_response.message);
      t.error_handler (`Postgres_error e)
    | _ ->
      (match t.state with
      | Connect -> handle_connect t msg
      | Parse -> handle_parse t msg
      | _ -> failwith "Not implemented yet")

  let connect ~user ?(password = "") ?database ~error_handler ~finish () =
    let rec handle_message msg =
      let t = Lazy.force t in
      handle_message' t msg
    and t =
      lazy
        { user
        ; password
        ; database
        ; error_handler
        ; backend_key_data = None
        ; reader = Parser.create handle_message
        ; writer = Serializer.create ()
        ; state = Connect
        ; ready_for_query = finish
        }
    in
    let t = Lazy.force t in
    let startup_message = Frontend.Startup_message.make ~user ?database () in
    Serializer.startup t.writer startup_message;
    Serializer.wakeup_writer t.writer;
    t

  let prepare conn ~statement ~name ?(oids = Array.of_list []) ~finish () =
    match conn.state with
    | Ready_for_query ->
      let prepare = { Frontend.Parse.name; statement; oids } in
      Serializer.parse conn.writer prepare;
      Serializer.sync conn.writer;
      Serializer.wakeup_writer conn.writer;
      conn.ready_for_query <- finish;
      conn.state <- Parse
    | _ -> failwith "Can't call prepare"

  let next_write_operation t = Serializer.next_operation t.writer
  let next_read_operation t = Parser.next_action t.reader

  let read t buf ~off ~len =
    Parser.feed t.reader ~buf ~off ~len Angstrom.Unbuffered.Incomplete

  let read_eof t buf ~off ~len =
    Parser.feed t.reader ~buf ~off ~len Angstrom.Unbuffered.Complete

  let yield_reader _t thunk = thunk ()
  let yield_writer t thunk = Serializer.yield_writer t.writer thunk
  let report_write_result t res = Serializer.report_write_result t.writer res

  let shutdown t =
    Parser.force_close t.reader;
    Serializer.close_and_drain t.writer

  let is_closed t = Parser.is_closed t.reader && Serializer.is_closed t.writer

  let report_exn t exn =
    shutdown t;
    t.error_handler (`Exn exn)
end
