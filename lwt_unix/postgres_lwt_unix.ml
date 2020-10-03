open Lwt.Syntax

let src = Logs.Src.create "postgres.lwt.unix"

module Log = (val Logs.src_log src : Logs.LOG)

type mode =
  | Regular
  | Tls of Tls.Config.client

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

let request_ssl socket =
  let ssl_avail, wakeup_ssl_avail = Lwt.wait () in
  let req = Postgres.Request_ssl.create (fun r -> Lwt.wakeup_later wakeup_ssl_avail r) in
  let rec loop () =
    match Postgres.Request_ssl.next_operation req with
    | `Write payload ->
      let* n = Lwt_unix.write socket payload 0 (Bytes.length payload) in
      Postgres.Request_ssl.report_write_result req n;
      loop ()
    | `Read ->
      let b = Bytes.create 1 in
      let* n = Lwt_unix.read socket b 0 1 in
      (match n with
      | 1 ->
        Postgres.Request_ssl.feed_char req (Bytes.get b 0);
        loop ()
      | _ -> failwith "Could not read postgres response")
    | `Stop -> Lwt.return_unit
    | `Fail msg -> failwith msg
  in
  Lwt.async (fun () -> loop ());
  ssl_avail

let connect ?(mode = Regular) user_info destination =
  let* socket =
    match destination with
    | Inet (host, port) -> connect_inet host port
    | Unix_domain p ->
      let socket = Lwt_unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
      let+ () = Lwt_unix.connect socket (Unix.ADDR_UNIX p) in
      socket
  in
  Lwt.catch
    (fun () ->
      let* socket =
        match mode with
        | Regular -> Lwt.return (Io.Socket.Regular socket)
        | Tls config ->
          let* avail = request_ssl socket in
          (match avail with
          | `Available ->
            let+ s = Tls_lwt.Unix.client_of_fd config socket in
            Io.Socket.Tls s
          | `Unavailable ->
            failwith
              "Requested a ssl connection, but the postgres server isn't able to support \
               connecting over ssl.")
      in
      Postgres_lwt.connect (Io.run socket) user_info)
    (fun exn ->
      Log.err (fun m ->
          m "Could not create postgres connection. %s" (Printexc.to_string exn));
      let* () = Lwt_unix.close socket in
      Lwt.fail exn)
