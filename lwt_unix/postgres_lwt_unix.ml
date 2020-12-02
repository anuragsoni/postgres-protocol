let ( >>= ) = Lwt.( >>= )
let ( >>=? ) = Lwt_result.( >>= )
let ( >>| ) = Lwt.( >|= )
let src = Logs.Src.create "postgres.lwt.unix"

module Log = (val Logs.src_log src : Logs.LOG)
module Error = Postgres.Error

type destination =
  | Unix_domain of string
  | Inet of string * int

let connect_inet host port =
  Lwt_unix.getaddrinfo host (Int.to_string port) [ Unix.AI_SOCKTYPE Unix.SOCK_STREAM ]
  >>= fun addr_info ->
  let rec run xs =
    match xs with
    | [] -> Lwt_result.fail (Error.of_string "Could not create socket connection")
    | x :: xs ->
      Lwt.catch
        (fun () ->
          let socket = Lwt_unix.socket x.Lwt_unix.ai_family x.ai_socktype x.ai_protocol in
          Lwt_unix.connect socket x.ai_addr >>| fun () -> Ok socket)
        (fun exn ->
          Log.warn (fun m ->
              m
                "Could not connect socket. Will try with next item in addrinfo list. \
                 Err: %s"
                (Printexc.to_string exn));
          run xs)
  in
  run addr_info
;;

let request_ssl socket =
  let ssl_avail, wakeup_ssl_avail = Lwt.wait () in
  let req =
    Postgres.Request_ssl.create (fun r -> Lwt.wakeup_later wakeup_ssl_avail (Ok r))
  in
  let rec loop () =
    match Postgres.Request_ssl.next_operation req with
    | `Write payload ->
      Lwt_unix.write socket payload 0 (Bytes.length payload)
      >>= fun n ->
      Postgres.Request_ssl.report_write_result req n;
      loop ()
    | `Read ->
      let b = Bytes.create 1 in
      Lwt_unix.read socket b 0 1
      >>= (function
      | 1 ->
        Postgres.Request_ssl.feed_char req (Bytes.get b 0);
        loop ()
      | n ->
        Lwt.wakeup_later
          wakeup_ssl_avail
          (Error (Error.of_string (Printf.sprintf "Expecting 1 byte, but received: %d" n)));
        Lwt.return_unit)
    | `Stop -> Lwt.return_unit
    | `Fail msg ->
      Lwt.wakeup_later wakeup_ssl_avail (Error (Error.of_string msg));
      Lwt.return_unit
  in
  Lwt.async (fun () -> loop ());
  ssl_avail
;;

let connect ?tls_config user_info destination =
  (match destination with
  | Inet (host, port) -> connect_inet host port
  | Unix_domain p ->
    let socket = Lwt_unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
    Lwt_unix.connect socket (Unix.ADDR_UNIX p) >>| fun () -> Ok socket)
  >>=? fun socket ->
  Lwt.catch
    (fun () ->
      (match tls_config with
      | None -> Lwt_result.return (Io.Socket.Regular socket)
      | Some config ->
        request_ssl socket
        >>=? (function
        | `Available ->
          Tls_lwt.Unix.client_of_fd config socket >>| fun s -> Ok (Io.Socket.Tls s)
        | `Unavailable ->
          Lwt_result.fail
            (Error.of_string
               "Requested a ssl connection, but the postgres server doesn't support \
                connecting over ssl.")))
      >>=? fun socket -> Postgres_lwt.startup (Io.run socket) user_info)
    (fun exn ->
      Log.err (fun m ->
          m "Could not create postgres connection. %s" (Printexc.to_string exn));
      Lwt_unix.close socket >>= fun () -> Lwt_result.fail (Error.of_exn exn))
;;
