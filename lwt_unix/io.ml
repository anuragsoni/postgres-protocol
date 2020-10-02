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


open Lwt.Syntax

let src = Logs.Src.create "postgres.lwt.io"

module Log = (val Logs.src_log src : Logs.LOG)

module Buffer : sig
  type t

  val create : int -> t
  val read : t -> (Lwt_bytes.t -> off:int -> len:int -> int) -> int
  val write : t -> (Lwt_bytes.t -> off:int -> len:int -> int Lwt.t) -> int Lwt.t
end = struct
  type t =
    { buffer : Lwt_bytes.t
    ; mutable off : int
    ; mutable len : int
    }

  let create size =
    let buffer = Lwt_bytes.create size in
    { buffer; off = 0; len = 0 }

  let compress t =
    if t.len = 0
    then (
      t.off <- 0;
      t.len <- 0)
    else if t.off > 0
    then (
      Lwt_bytes.blit t.buffer t.off t.buffer 0 t.len;
      t.off <- 0)

  let read t f =
    let n = f t.buffer ~off:t.off ~len:t.len in
    t.off <- t.off + n;
    t.len <- t.len - n;
    if t.len = 0 then t.off <- 0;
    n

  let write t f =
    compress t;
    let* n = f t.buffer ~off:(t.off + t.len) ~len:(Lwt_bytes.length t.buffer - t.len) in
    t.len <- t.len + n;
    Lwt.return n
end

let close_if_open socket =
  if Lwt_unix.state socket <> Lwt_unix.Closed
  then
    Lwt.catch
      (fun () -> Lwt_unix.close socket)
      (fun exn ->
        Log.warn (fun m -> m "Error while closing socket: %s" (Printexc.to_string exn));
        Lwt.return_unit)
  else Lwt.return_unit

let shutdown_if_open socket =
  if Lwt_unix.state socket <> Lwt_unix.Closed
  then (
    try Lwt_unix.shutdown socket Lwt_unix.SHUTDOWN_RECEIVE with
    | Unix.Unix_error (Unix.ENOTCONN, _, _) -> ())

let read socket buffer =
  Lwt.catch
    (fun () ->
      let+ count =
        Buffer.write buffer (fun buf ~off ~len -> Lwt_bytes.read socket buf off len)
      in
      if count = 0 then `Eof else `Ok count)
    (fun exn ->
      Lwt.async (fun () -> close_if_open socket);
      Lwt.fail exn)

open Postgres

let run socket conn =
  let read_buffer = Buffer.create 0x1000 in
  let read_loop_finish, notify_read_loop_finish = Lwt.wait () in
  let read_loop () =
    let rec aux () =
      match Connection.next_read_operation conn with
      | `Read ->
        let* res = read socket read_buffer in
        (match res with
        | `Eof ->
          ignore
            (Buffer.read read_buffer (fun buf ~off ~len ->
                 Connection.read_eof conn buf ~off ~len));
          aux ()
        | `Ok _ ->
          ignore
            (Buffer.read read_buffer (fun buf ~off ~len ->
                 Connection.read conn buf ~off ~len));
          aux ())
      | `Close ->
        Lwt.wakeup_later notify_read_loop_finish ();
        shutdown_if_open socket;
        Lwt.return_unit
    in
    Lwt.async (fun () ->
        Lwt.catch aux (fun exn ->
            Connection.report_exn conn exn;
            Lwt.return_unit))
  in
  let writev = Faraday_lwt_unix.writev_of_fd socket in
  let write_loop_finish, notify_write_loop_finish = Lwt.wait () in
  let rec write_loop () =
    let rec aux () =
      match Connection.next_write_operation conn with
      | `Write iovecs ->
        let* res = writev iovecs in
        Connection.report_write_result conn res;
        aux ()
      | `Yield ->
        Connection.yield_writer conn write_loop;
        Lwt.return_unit
      | `Close _ ->
        Lwt.wakeup_later notify_write_loop_finish ();
        Lwt.return_unit
    in
    Lwt.async (fun () ->
        Lwt.catch aux (fun exn ->
            Connection.report_exn conn exn;
            Lwt.return_unit))
  in
  read_loop ();
  write_loop ();
  Lwt.async (fun () ->
      let* () = Lwt.join [ read_loop_finish; write_loop_finish ] in
      close_if_open socket)