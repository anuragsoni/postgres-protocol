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
