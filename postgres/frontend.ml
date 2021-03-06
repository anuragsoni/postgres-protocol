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
open Sexplib0.Sexp_conv

type protocol_version = V3_0 [@@deriving sexp_of]

let write_protocol_version f = function
  | V3_0 ->
    Faraday.BE.write_uint16 f 3;
    Faraday.BE.write_uint16 f 0
;;

let write_cstr f s =
  Faraday.write_string f s;
  Faraday.write_uint8 f 0
;;

module Startup_message = struct
  let ident = None

  type t =
    { user : string
    ; database : string option
    ; protocol_version : protocol_version
    }
  [@@deriving sexp_of]

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
  ;;

  let size { user; database; _ } =
    let user_len = 4 + 1 + String.length user + 1 in
    let database_len =
      match database with
      | None -> 0
      | Some d -> 8 + 1 + String.length d + 1
    in
    4 + user_len + database_len + 1
  ;;
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
  ;;

  let write f { name; statement; oids } =
    write_cstr f (Optional_string.to_string name);
    write_cstr f statement;
    Faraday.BE.write_uint16 f (Array.length oids);
    Array.iter (fun oid -> Faraday.BE.write_uint32 f (Oid.to_int32 oid)) oids
  ;;
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
  ;;

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
  ;;

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
  ;;
end

module Execute = struct
  let ident = Some 'E'

  type t =
    { name : Optional_string.t
    ; max_rows : [ `Unlimited | `Count of Positive_int32.t ]
    }

  let make ?(name = "") max_rows () = { name = Optional_string.of_string name; max_rows }
  let size { name; _ } = Optional_string.length name + 1 + 4

  let write f { name; max_rows } =
    write_cstr f (Optional_string.to_string name);
    let count =
      match max_rows with
      | `Unlimited -> 0l
      | `Count p -> Positive_int32.to_int32 p
    in
    Faraday.BE.write_uint32 f count
  ;;
end

module Close = struct
  let ident = Some 'C'

  type kind =
    | Portal
    | Statement

  type t =
    { kind : kind
    ; name : string
    }

  let portal name = { kind = Portal; name }
  let statement name = { kind = Statement; name }
  let size { name; _ } = 1 + String.length name + 1

  let write f { kind; name } =
    Faraday.write_char
      f
      (match kind with
      | Portal -> 'P'
      | Statement -> 'S');
    write_cstr f name
  ;;
end

let sync = 'S'
let terminate = 'X'
let flush = 'H'

module Private = struct
  module Serializer = struct
    type t =
      { writer : Faraday.t
      ; mutable wakeup_writer : (unit -> unit) option
      ; mutable drained_bytes : int
      }

    let create ?(size = 0x1000) () =
      { writer = Faraday.create size; wakeup_writer = None; drained_bytes = 0 }
    ;;

    let yield_writer t thunk =
      if Faraday.is_closed t.writer then failwith "Serializer is closed";
      match t.wakeup_writer with
      | None -> t.wakeup_writer <- Some thunk
      | Some _ -> failwith "Only one write callback can be registered at a time"
    ;;

    let wakeup_writer t =
      let thunk = t.wakeup_writer in
      t.wakeup_writer <- None;
      Option.iter (fun t -> t ()) thunk
    ;;

    let is_closed t = Faraday.is_closed t.writer
    let drain t = Faraday.drain t.writer

    let close_and_drain t =
      Faraday.close t.writer;
      t.drained_bytes <- drain t
    ;;

    let report_write_result t res =
      match res with
      | `Closed -> close_and_drain t
      | `Ok n -> Faraday.shift t.writer n
    ;;

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
    ;;

    let write_ident_only ident t =
      Faraday.write_char t.writer ident;
      Faraday.BE.write_uint32 t.writer 4l
    ;;

    let startup t msg = write (module Startup_message) msg t
    let password t msg = write (module Password_message) msg t
    let parse t msg = write (module Parse) msg t
    let bind t msg = write (module Bind) msg t
    let execute t msg = write (module Execute) msg t
    let sync t = write_ident_only sync t
    let terminate t = write_ident_only terminate t
    let close t msg = write (module Close) msg t
    let flush t = write_ident_only flush t

    let next_operation t =
      match Faraday.operation t.writer with
      | `Close -> `Close t.drained_bytes
      | `Yield -> `Yield
      | `Writev iovecs -> `Write iovecs
    ;;
  end
end
