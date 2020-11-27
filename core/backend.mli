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

module Header : sig
  type t =
    { length : int
    ; kind : char
    }
  [@@deriving sexp_of]
end

module Auth : sig
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
  [@@deriving sexp_of]
end

module Backend_key_data : sig
  type t =
    { pid : Types.Process_id.t
    ; secret : int32
    }
  [@@deriving sexp_of]
end

module Error_or_notice_kind : sig
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
end

module Error_response : sig
  type t = (Error_or_notice_kind.t * Types.Optional_string.t) list [@@deriving sexp_of]
end

module Notice_response : sig
  type t = (Error_or_notice_kind.t * Types.Optional_string.t) list [@@deriving sexp_of]
end

module Parameter_status : sig
  type t =
    { name : string
    ; value : string
    }
  [@@deriving sexp_of]
end

module Ready_for_query : sig
  type t =
    | Idle
    | Transaction_block
    | Failed_transaction
  [@@deriving sexp_of]
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

val parse : message Angstrom.t
