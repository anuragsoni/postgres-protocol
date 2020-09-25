open Lwt.Syntax

let create_socket host port =
  let* addresses =
    Lwt_unix.getaddrinfo host (Int.to_string port) [ Unix.(AI_FAMILY PF_INET) ]
  in
  let socket = Lwt_unix.socket Unix.(PF_INET) Unix.SOCK_STREAM 0 in
  let+ () = Lwt_unix.connect socket (List.hd addresses).ai_addr in
  socket

let connect socket ~user ~password () =
  let finished, wakeup_finished = Lwt.wait () in
  Lwt.on_success finished (fun () ->
      Logs.info (fun m -> m "Logged in to postgres database"));
  let conn =
    Postgres_protocol.Connection.create
      ~user
      ~password
      (fun () -> Lwt.wakeup_later wakeup_finished ())
      (fun _error -> (* TODO: Handle errors *) ())
      ()
  in
  let* () =
    Lwt.map
      ignore
      (Gluten_lwt_unix.Client.create
         ~read_buffer_size:0x1000
         ~protocol:(module Postgres_protocol.Connection)
         conn
         socket)
  in
  finished

let run () =
  let* socket = create_socket "localhost" 5432 in
  connect socket ~user:"asoni" ~password:"password" ()

let () =
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level ~all:true (Some Debug);
  Fmt_tty.setup_std_outputs ();
  Lwt_main.run (run ())
