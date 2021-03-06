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

open Postgres

let wakeup_exn p w err = if Lwt.is_sleeping p then Lwt.wakeup_later w (Error err) else ()
let wakeup_if_empty p w r = if Lwt.is_sleeping p then Lwt.wakeup_later w (Ok r) else ()

include Postgres.Connection.Make (struct
  type 'a t = ('a, Error.t) Lwt_result.t

  let return = Lwt_result.return
  let ( >>= ) = Lwt_result.( >>= )

  let of_cps cps =
    let promise, wakeup = Lwt.wait () in
    cps (wakeup_exn promise wakeup) (wakeup_if_empty promise wakeup);
    promise
  ;;

  module Sequencer = struct
    type 'a future = ('a, Error.t) Lwt_result.t

    type 'a t =
      { mutex : Lwt_mutex.t
      ; conn : 'a
      }

    let create conn = { conn; mutex = Lwt_mutex.create () }
    let enqueue t f = Lwt_mutex.with_lock t.mutex (fun () -> f t.conn)
  end
end)
