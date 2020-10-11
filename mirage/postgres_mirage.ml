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

open Lwt.Infix

let src = Logs.Src.create "postgres.mirage"

module Log = (val Logs.src_log src : Logs.LOG)

type destination =
  | Domain of [ `host ] Domain_name.t * int
  | Ipv4 of Ipaddr.V4.t * int

module Make
    (STACK : Mirage_stack.V4)
    (TIME : Mirage_time.S)
    (MCLOCK : Mirage_clock.MCLOCK)
    (RANDOM : Mirage_random.S) =
struct
  module Dns = Dns_client_mirage.Make (RANDOM) (TIME) (MCLOCK) (STACK)
  module TLS = Tls_mirage.Make (STACK.TCPV4)

  module Io = struct
    open Postgres

    let run flow conn =
      let read_loop_finish, notify_read_loop_finish = Lwt.wait () in
      let rec read_loop () =
        let rec aux () =
          match Connection.next_read_operation conn with
          | `Yield ->
            Connection.yield_reader conn read_loop;
            Lwt.return_unit
          | `Close ->
            Lwt.wakeup_later notify_read_loop_finish ();
            STACK.TCPV4.close flow
          | `Read ->
            Lwt.catch
              (fun () ->
                STACK.TCPV4.read flow
                >>= function
                | Ok (`Data cstruct) ->
                  let buf = Cstruct.to_bigarray cstruct in
                  ignore (Connection.read conn buf ~off:0 ~len:(Bigstringaf.length buf));
                  aux ()
                | Ok `Eof ->
                  let buf = Bigstringaf.create 0 in
                  ignore (Connection.read_eof conn buf ~off:0 ~len:0);
                  aux ()
                | Error err ->
                  let msg = Format.asprintf "%a" STACK.TCPV4.pp_error err in
                  failwith msg)
              (fun exn -> STACK.TCPV4.close flow >>= fun () -> raise exn)
        in
        Lwt.async (fun () ->
            Lwt.catch aux (fun exn ->
                Connection.report_exn conn exn;
                Lwt.return_unit))
      in
      let writev = STACK.TCPV4.writev flow in
      let write_loop_finish, notify_write_loop_finish = Lwt.wait () in
      let rec write_loop () =
        let rec aux () =
          match Connection.next_write_operation conn with
          | `Close _ ->
            Lwt.wakeup_later notify_write_loop_finish ();
            Lwt.return_unit
          | `Yield ->
            Connection.yield_writer conn write_loop;
            Lwt.return_unit
          | `Write iovecs ->
            let cv =
              List.map
                (fun { Faraday.buffer; off; len } -> Cstruct.of_bigarray buffer ~off ~len)
                iovecs
            in
            Lwt.catch
              (fun () ->
                writev cv
                >>= function
                | Ok () ->
                  Connection.report_write_result conn (`Ok (Cstruct.lenv cv));
                  aux ()
                | Error err ->
                  let msg = Format.asprintf "%a" STACK.TCPV4.pp_write_error err in
                  failwith msg)
              (fun exn -> STACK.TCPV4.close flow >>= fun () -> raise exn)
        in
        Lwt.async (fun () ->
            Lwt.catch aux (fun exn ->
                Connection.report_exn conn exn;
                Lwt.return_unit))
      in
      read_loop ();
      write_loop ();
      Lwt.async (fun () ->
          Lwt.join [ read_loop_finish; write_loop_finish ]
          >>= fun () -> STACK.TCPV4.close flow)
  end

  let resolve dns destination =
    let open Lwt_result.Syntax in
    match destination with
    | Domain (d, port) ->
      let+ i = Dns.gethostbyname dns d in
      i, port
    | Ipv4 (ip, p) -> Lwt_result.return (ip, p)

  let connect_inet stack dns destination =
    let open Lwt.Infix in
    resolve dns destination
    >>= function
    | Ok destination ->
      STACK.TCPV4.create_connection stack destination
      >>= (function
      | Ok res -> Lwt.return res
      | Error err ->
        let err_msg = Format.asprintf "%a" STACK.TCPV4.pp_error err in
        Log.err (fun m -> m "Failed to establish connection: %S" err_msg);
        Lwt.fail_with err_msg)
    | Error (`Msg msg) ->
      Log.err (fun m -> m "Failed to resolve destination: %S" msg);
      Lwt.fail_with msg

  let create stack =
    let dns = Dns.create stack in
    fun user_info destination ->
      let open Lwt.Infix in
      connect_inet (STACK.tcpv4 stack) dns destination
      >>= fun flow -> Postgres_lwt.connect (fun conn -> Io.run flow conn) user_info
end
