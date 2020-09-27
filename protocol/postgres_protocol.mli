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

val src : Logs.src

module Types : sig
  module Process_id : sig
    type t

    val to_int32 : t -> int32
    val of_int32 : int32 -> t option
    val of_int32_exn : int32 -> t
  end

  module Statement_or_portal : sig
    type t =
      | Statement
      | Portal

    val to_char : t -> char
    val of_char : char -> t
  end

  module Positive_int32 : sig
    type t

    val of_int32_exn : int32 -> t
    val to_int32 : t -> int32
  end

  module Optional_string : sig
    type t

    val empty : t
    val of_string : string -> t
    val to_string : t -> string
    val is_empty : t -> bool
    val length : t -> int
  end

  module Oid : sig
    type t

    val of_int32 : int32 -> t
    val of_int_exn : int -> t
    val to_int32 : t -> int32
  end

  module Format_code : sig
    type t =
      [ `Binary
      | `Text
      ]

    val of_int : int -> [> `Binary | `Text ] option
    val to_int : [< `Binary | `Text ] -> int
  end
end

module Auth : sig
  module Md5 : sig
    val hash : username:string -> password:string -> salt:string -> string
  end
end

type protocol_version = V3_0

module Frontend : sig
  module Startup_message : sig
    type t =
      { user : string
      ; database : string option
      ; protocol_version : protocol_version
      }

    val make : user:string -> ?database:string -> unit -> t
  end

  module Password_message : sig
    type t = Md5 of string
  end

  module Parse : sig
    type t =
      { name : Types.Optional_string.t
      ; statement : string
      ; oids : Types.Oid.t Array.t
      }
  end

  module Bind : sig
    type parameter =
      { format_code : Types.Format_code.t
      ; parameter : string option
      }

    val make_param : Types.Format_code.t -> ?parameter:string -> unit -> parameter

    type t =
      { destination : Types.Optional_string.t
      ; statement : Types.Optional_string.t
      ; parameters : parameter Array.t
      ; result_formats : Types.Format_code.t Array.t
      }

    val make
      :  ?destination:string
      -> ?statement:string
      -> ?parameters:parameter array
      -> ?result_formats:Types.Format_code.t array
      -> unit
      -> t
  end

  module Execute : sig
    type t =
      { name : Types.Optional_string.t
      ; max_rows : [ `Count of Types.Positive_int32.t | `Unlimited ]
      }

    val make
      :  ?name:string
      -> [ `Count of Types.Positive_int32.t | `Unlimited ]
      -> unit
      -> t
  end

  module Close : sig
    type t =
      { kind : Types.Statement_or_portal.t
      ; name : Types.Optional_string.t
      }
  end

  module Describe : sig
    type t =
      { kind : Types.Statement_or_portal.t
      ; name : Types.Optional_string.t
      }
  end

  module Copy_fail : sig
    type t

    val of_string : string -> t
  end
end

module Backend : sig
  module Header : sig
    type t =
      { length : int
      ; kind : char
      }
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
  end

  module Backend_key_data : sig
    type t =
      { pid : Types.Process_id.t
      ; secret : Int32.t
      }
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
    type t =
      { code : Error_or_notice_kind.t
      ; message : Types.Optional_string.t
      }
  end

  module Notice_response : sig
    type t =
      { code : Error_or_notice_kind.t
      ; message : Types.Optional_string.t
      }
  end

  module Parameter_status : sig
    type t =
      { name : string
      ; value : string
      }
  end

  module Ready_for_query : sig
    type t =
      | Idle
      | Transaction_block
      | Failed_transaction
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
end

module Serializer : sig
  type t

  val create : ?size:int -> unit -> t
  val yield_writer : t -> (unit -> unit) -> unit
  val wakeup_writer : t -> unit
  val is_closed : t -> bool
  val drain : t -> int
  val close_and_drain : t -> unit
  val report_write_result : t -> [< `Closed | `Ok of int ] -> unit
  val startup : t -> Frontend.Startup_message.t -> unit
  val password : t -> Frontend.Password_message.t -> unit
  val parse : t -> Frontend.Parse.t -> unit
  val bind : t -> Frontend.Bind.t -> unit
  val execute : t -> Frontend.Execute.t -> unit
  val close : t -> Frontend.Close.t -> unit
  val describe : t -> Frontend.Describe.t -> unit
  val copy_fail : t -> Frontend.Copy_fail.t -> unit
  val flush : t -> unit
  val sync : t -> unit
  val terminate : t -> unit
  val copy_done : t -> unit

  val next_operation
    :  t
    -> [> `Close of int | `Write of Faraday.bigstring Faraday.iovec list | `Yield ]
end

module Parser : sig
  type t

  val create : (Backend.message -> unit) -> t
  val next_action : t -> [> `Close | `Read ]

  val feed
    :  t
    -> buf:Angstrom.bigstring
    -> off:int
    -> len:int
    -> Angstrom.Unbuffered.more
    -> int

  val is_closed : t -> bool
  val force_close : t -> unit
end

module Connection : sig
  exception Auth_method_not_implemented of string

  type error =
    [ `Exn of exn
    | `Postgres_error of Backend.Error_response.t
    | `Parse_error of string
    ]

  type error_handler = error -> unit
  type t

  val connect
    :  user:string
    -> ?password:string
    -> ?database:string
    -> error_handler:error_handler
    -> finish:(unit -> unit)
    -> unit
    -> t

  val prepare
    :  t
    -> statement:string
    -> name:string
    -> ?oids:Types.Oid.t array
    -> finish:(unit -> unit)
    -> unit
    -> unit

  val execute
    :  t
    -> ?name:string
    -> ?statement:string
    -> ?parameters:Frontend.Bind.parameter array
    -> on_data_row:(string option list -> unit)
    -> (unit -> unit)
    -> unit

  val close : t -> unit

  val next_write_operation
    :  t
    -> [> `Close of int | `Write of Faraday.bigstring Faraday.iovec list | `Yield ]

  val next_read_operation : t -> [> `Close | `Read ]
  val read : t -> Angstrom.bigstring -> off:int -> len:int -> int
  val read_eof : t -> Angstrom.bigstring -> off:int -> len:int -> int
  val yield_reader : t -> (unit -> unit) -> unit
  val report_write_result : t -> [< `Closed | `Ok of int ] -> unit
  val report_exn : t -> exn -> unit
  val is_closed : t -> bool
  val shutdown : t -> unit
  val yield_writer : t -> (unit -> unit) -> unit
end
