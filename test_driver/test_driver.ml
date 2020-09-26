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
    Postgres_protocol.Connection.connect
      ~user
      ~password
      ~finish:(fun () -> Lwt.wakeup_later wakeup_finished ())
      ~error_handler:(fun e ->
        match e with
        | `Exn exn -> Logs.err (fun m -> m "%s" (Printexc.to_string exn))
        | `Parse_error msg -> Logs.err (fun m -> m "Parse error: %S" msg)
        | `Postgres_error e ->
          Logs.err (fun m ->
              m
                "postgres_error: %S"
                (e.Postgres_protocol.Backend.Error_response.message
                |> Postgres_protocol.Types.Optional_string.to_string)))
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
