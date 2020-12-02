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
open Angstrom
open Sexplib0.Sexp_conv
open Types

let parse_cstr = Angstrom.(take_while (fun c -> c <> '\x00') <* char '\x00')

module Header = struct
  type t =
    { length : int
    ; kind : char
    }
  [@@deriving sexp_of]

  let parse =
    let parse_len =
      BE.any_int32
      >>= fun l ->
      let l = Int32.to_int l in
      if l < 4 then fail (Printf.sprintf "Invalid payload length: %d" l) else return l
    in
    lift2 (fun kind length -> { kind; length }) any_char parse_len <?> "MESSAGE_HEADER"
  ;;
end

module Auth = struct
  type t =
    | Ok
    | KerberosV5
    | CleartextPassword
    | Md5Password of (string[@sexp.opaque])
    | SCMCredential
    | GSS
    | SSPI
    | GSSContinue of (string[@sexp.opaque])
    | SASL of (string[@sexp.opaque])
    | SASLContinue of (string[@sexp.opaque])
    | SASLFinal of (string[@sexp.opaque])
  [@@deriving sexp_of]

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
  ;;
end

module Backend_key_data = struct
  type t =
    { pid : Process_id.t
    ; secret : (int32[@sexp.opaque])
    }
  [@@deriving sexp_of]

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
  ;;
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
  [@@deriving sexp_of]

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
  ;;
end

module Make_error_notice (K : sig
  val label : string
end) =
struct
  type t = (Error_or_notice_kind.t * Optional_string.t) list [@@deriving sexp_of]

  let parse_message_string =
    lift Optional_string.of_string parse_cstr <?> Printf.sprintf "%s_MESSAGE" K.label
  ;;

  let parse_message =
    Error_or_notice_kind.parse
    >>= function
    | Unknown '\x00' as code -> return (code, Optional_string.empty)
    | code -> lift (fun message -> code, message) parse_message_string
  ;;

  let parse _ = many_till parse_message (char '\x00') <?> K.label
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
  [@@deriving sexp_of]

  let parse _header =
    lift2 (fun name value -> { name; value }) parse_cstr parse_cstr <?> "PARAMETER_STATUS"
  ;;
end

module Ready_for_query = struct
  type t =
    | Idle
    | Transaction_block
    | Failed_transaction
  [@@deriving sexp_of]

  let parse _header =
    any_char
    >>= (function
          | 'I' -> return Idle
          | 'T' -> return Transaction_block
          | 'E' -> return Failed_transaction
          | c -> fail @@ Printf.sprintf "Unknown response for ReadyForQuery: %C" c)
    <?> "READY_FOR_QUERY"
  ;;
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
  ;;
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
  | CloseComplete
  | DataRow of string option list
  | UnknownMessage of char

let parse =
  Header.parse
  <* commit
  >>= fun ({ kind; length } as header) ->
  Logger.debug (fun m -> m "Message received of kind: %C" kind);
  match kind with
  | 'R' -> lift (fun m -> Auth m) @@ Auth.parse header
  | 'K' -> lift (fun m -> BackendKeyData m) @@ Backend_key_data.parse header
  | 'E' -> lift (fun m -> ErrorResponse m) @@ Error_response.parse header
  | 'N' -> lift (fun m -> NoticeResponse m) @@ Notice_response.parse header
  | 'S' -> lift (fun m -> ParameterStatus m) @@ Parameter_status.parse header
  | 'Z' -> lift (fun m -> ReadyForQuery m) @@ Ready_for_query.parse header
  | '1' -> return ParseComplete
  | '2' -> return BindComplete
  | '3' -> return CloseComplete
  | 'D' -> lift (fun m -> DataRow m) @@ Data_row.parse header
  | c ->
    Logger.warn (fun m -> m "Received an unknown message with ident: %C" c);
    take (length - 4) *> (return @@ UnknownMessage c)
;;

module Private = struct
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
    ;;

    let next_action t =
      match t.parse_state with
      | _ when t.closed -> `Close
      | U.Done _ -> `Read
      | Partial _ -> `Read
      | Fail (_, _, _msg) -> `Close
    ;;

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
    ;;

    let feed t ~buf ~off ~len more =
      let committed = parse t ~buf ~off ~len more in
      (match more with
      | U.Complete -> t.closed <- true
      | Incomplete -> ());
      committed
    ;;

    let is_closed t = t.closed
    let force_close t = t.closed <- true
  end
end
