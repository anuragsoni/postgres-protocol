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

module Process_id : sig
  type t [@@deriving sexp_of]

  val to_int32 : t -> int32
  val of_int32 : int32 -> t option
  val of_int32_exn : int32 -> t
end

module Statement_or_portal : sig
  type t =
    | Statement
    | Portal
  [@@deriving sexp_of]

  val to_char : t -> char
  val of_char : char -> t
end

module Positive_int32 : sig
  type t [@@deriving sexp_of]

  val of_int32_exn : int32 -> t
  val to_int32 : t -> int32
end

module Optional_string : sig
  type t [@@deriving sexp_of]

  val empty : t
  val of_string : string -> t
  val to_string : t -> string
  val is_empty : t -> bool
  val length : t -> int
end

module Oid : sig
  type t [@@deriving sexp_of]

  val of_int32 : int32 -> t
  val of_int_exn : int -> t
  val to_int32 : t -> int32
end

module Format_code : sig
  type t =
    [ `Binary
    | `Text
    ]
  [@@deriving sexp_of]

  val of_int : int -> [> `Binary | `Text ] option
  val to_int : [< `Binary | `Text ] -> int
end
