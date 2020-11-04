(*---------------------------------------------------------------------------- Copyright
  (c) 2018 Inhabited Type LLC. Copyright (c) 2018 Anton Bachin

  All rights reserved.

  Redistribution and use in source and binary forms, with or without modification, are
  permitted provided that the following conditions are met:

  1. Redistributions of source code must retain the above copyright notice, this list of
  conditions and the following disclaimer.

  2. Redistributions in binary form must reproduce the above copyright notice, this list
  of conditions and the following disclaimer in the documentation and/or other materials
  provided with the distribution.

  3. Neither the name of the author nor the names of his contributors may be used to
  endorse or promote products derived from this software without specific prior written
  permission.

  THIS SOFTWARE IS PROVIDED BY THE CONTRIBUTORS ``AS IS'' AND ANY EXPRESS OR IMPLIED
  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
  FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR
  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
  ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
  ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  ----------------------------------------------------------------------------*)

open Core
open Async
module Unix = Core.Unix

let src = Logs.Src.create "postgres.async.io"

module Log = (val Logs.src_log src : Logs.LOG)

module Buffer : sig
  type t

  val create : int -> t
  val read : t -> (Bigstring.t -> off:int -> len:int -> int) -> int

  val write
    :  t
    -> (Bigstring.t -> off:int -> len:int -> [ `Eof | `Ok of int ] Deferred.t)
    -> [ `Eof | `Ok of int ] Deferred.t
end = struct
  type t =
    { buffer : Bigstring.t
    ; mutable off : int
    ; mutable len : int
    }

  let create size =
    let buffer = Bigstring.create size in
    { buffer; off = 0; len = 0 }

  let compress t =
    if t.len = 0
    then (
      t.off <- 0;
      t.len <- 0)
    else if t.off > 0
    then (
      Bigstring.blit ~src:t.buffer ~src_pos:t.off ~dst:t.buffer ~dst_pos:0 ~len:t.len;
      t.off <- 0)

  let read t f =
    let n = f t.buffer ~off:t.off ~len:t.len in
    t.off <- t.off + n;
    t.len <- t.len - n;
    if t.len = 0 then t.off <- 0;
    n

  let write t f =
    compress t;
    match%map
      f t.buffer ~off:(t.off + t.len) ~len:(Bigstring.length t.buffer - t.len)
    with
    | `Eof -> `Eof
    | `Ok n as r ->
      t.len <- t.len + n;
      r
end

module Socket = struct
  let read reader buffer =
    Buffer.write buffer (fun buf ~off ~len ->
        let bstr = Bigsubstring.create buf ~pos:off ~len in
        Reader.read_bigsubstring reader bstr)

  let writev writer iovecs =
    match Writer.is_closed writer with
    (* schedule_iovecs will throw if the writer is closed. Checking for the writer status
       here avoids that and allows to report the closed status to httpaf. *)
    | true -> `Closed
    | false ->
      let total_bytes =
        List.fold_left iovecs ~init:0 ~f:(fun acc { Faraday.buffer; off; len } ->
            Writer.write_bigstring writer buffer ~pos:off ~len;
            acc + len)
      in
      `Ok total_bytes
end

open Postgres

let run conn reader writer =
  let read_buffer = Buffer.create 0x1000 in
  let read_loop_finished = Ivar.create () in
  let rec read_loop () =
    match Connection.next_read_operation conn with
    | `Read ->
      Socket.read reader read_buffer
      >>> (function
      | `Eof ->
        ignore
          (Buffer.read read_buffer (fun buf ~off ~len ->
               Connection.read_eof conn buf ~off ~len)
            : int);
        read_loop ()
      | `Ok _ ->
        ignore
          (Buffer.read read_buffer (fun buf ~off ~len ->
               Connection.read conn buf ~off ~len)
            : int);
        read_loop ())
    | `Yield -> Connection.yield_reader conn read_loop
    | `Close -> Ivar.fill_if_empty read_loop_finished ()
  in
  let write_loop_finished = Ivar.create () in
  let rec write_loop () =
    match Connection.next_write_operation conn with
    | `Write iovecs ->
      let result = Socket.writev writer iovecs in
      Connection.report_write_result conn result;
      write_loop ()
    | `Yield -> Connection.yield_writer conn write_loop
    | `Close _ -> Ivar.fill_if_empty write_loop_finished ()
  in
  let monitor = Monitor.create ~here:[%here] ~name:"Postgres.Async" () in
  Scheduler.within ~monitor read_loop;
  Scheduler.within ~monitor write_loop;
  Monitor.detach_and_iter_errors monitor ~f:(fun exn -> Connection.report_exn conn exn);
  Deferred.all_unit [ Ivar.read write_loop_finished; Ivar.read read_loop_finished ]
