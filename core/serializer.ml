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

let startup t msg = write (module Frontend.Startup_message) msg t
let password t msg = write (module Frontend.Password_message) msg t
let parse t msg = write (module Frontend.Parse) msg t
let bind t msg = write (module Frontend.Bind) msg t
let execute t msg = write (module Frontend.Execute) msg t
let sync t = write_ident_only Frontend.sync t
let terminate t = write_ident_only Frontend.terminate t

let next_operation t =
  match Faraday.operation t.writer with
  | `Close -> `Close t.drained_bytes
  | `Yield -> `Yield
  | `Writev iovecs -> `Write iovecs
;;
