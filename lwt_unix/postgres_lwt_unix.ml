open Lwt.Syntax

let src = Logs.Src.create "postgres.lwt.unix"

module Log = (val Logs.src_log src : Logs.LOG)

type destination =
  | Unix_domain of string
  | Inet of string * int

let connect_inet host port =
  let* addr_info =
    Lwt_unix.getaddrinfo host (Int.to_string port) [ Unix.AI_SOCKTYPE Unix.SOCK_STREAM ]
  in
  let rec run xs =
    match xs with
    | [] -> failwith "Could not create socket connection"
    | x :: xs ->
      Lwt.catch
        (fun () ->
          let socket = Lwt_unix.socket x.Lwt_unix.ai_family x.ai_socktype x.ai_protocol in
          let+ () = Lwt_unix.connect socket x.ai_addr in
          socket)
        (fun exn ->
          Log.warn (fun m ->
              m
                "Could not connect socket. Will try with next item in addrinfo list. \
                 Err: %s"
                (Printexc.to_string exn));
          run xs)
  in
  run addr_info

let connect user_info destination =
  let* socket =
    match destination with
    | Inet (host, port) -> connect_inet host port
    | Unix_domain p ->
      let socket = Lwt_unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
      let+ () = Lwt_unix.connect socket (Unix.ADDR_UNIX p) in
      socket
  in
  Postgres_lwt.connect (Io.run socket) user_info
