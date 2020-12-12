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

type protocol_version = V3_0 [@@deriving sexp_of]

module Startup_message : sig
  type t =
    { user : string
    ; database : string option
    ; protocol_version : protocol_version
    }
  [@@deriving sexp_of]

  val make : user:string -> ?database:string -> unit -> t
end

module Password_message : sig
  type t = Md5_or_plain of string
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
  type t

  val portal : string -> t
  val statement : string -> t
end

module Private : sig
  module Serializer : sig
    type t

    val close_and_drain : t -> unit
    val create : ?size:int -> unit -> t
    val yield_writer : t -> (unit -> unit) -> unit
    val wakeup_writer : t -> unit
    val is_closed : t -> bool
    val report_write_result : t -> [ `Ok of int | `Closed ] -> unit
    val startup : t -> Startup_message.t -> unit
    val password : t -> Password_message.t -> unit
    val parse : t -> Parse.t -> unit
    val bind : t -> Bind.t -> unit
    val execute : t -> Execute.t -> unit
    val sync : t -> unit
    val terminate : t -> unit
    val close : t -> Close.t -> unit
    val flush : t -> unit

    val next_operation
      :  t
      -> [ `Write of Faraday.bigstring Faraday.iovec list | `Yield | `Close of int ]
  end
end
