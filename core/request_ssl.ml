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
