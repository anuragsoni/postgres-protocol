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

module Process_id = struct
  type t = Int32.t

  let pp = Fmt.int32
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

  let to_string = function
    | Statement -> "Statement"
    | Portal -> "Portal"

  let pp = Fmt.of_to_string to_string

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

  let pp = Fmt.int32

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

  let pp = Fmt.string
  let empty = ""
  let of_string = Fun.id
  let to_string = Fun.id
  let is_empty t = t = ""
  let length = String.length
end

module Oid = struct
  type t = Int32.t

  let pp = Fmt.int32
  let of_int32 = Fun.id
  let of_int_exn = Int32.of_int
  let to_int32 = Fun.id
end

module Format_code = struct
  type t =
    [ `Text
    | `Binary
    ]

  let to_string = function
    | `Text -> "Text"
    | `Binary -> "Binary"

  let pp = Fmt.of_to_string to_string

  let of_int = function
    | 0 -> Some `Text
    | 1 -> Some `Binary
    | _ -> None

  let to_int = function
    | `Text -> 0
    | `Binary -> 1
end
