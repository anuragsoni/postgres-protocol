open Postgres_protocol
open Lwt.Syntax

let create_socket host port =
  let* addresses =
    Lwt_unix.getaddrinfo host (Int.to_string port) [ Unix.(AI_FAMILY PF_INET) ]
  in
  let socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let+ () = Lwt_unix.connect socket (List.hd addresses).ai_addr in
  socket

let connect ~host ~port ~user ?password ?database ~error_handler () =
  let* socket = create_socket host port in
  let finished, wakeup_finished = Lwt.wait () in
  let conn =
    Connection.connect
      ~user
      ?password
      ?database
      ~finish:(fun () -> Lwt.wakeup_later wakeup_finished ())
      ~error_handler
      ()
  in
  let* _ =
    Gluten_lwt_unix.Client.create
      ~read_buffer_size:0x1000
      ~protocol:(module Connection)
      conn
      socket
  in
  let+ () = finished in
  conn

let prepare conn ~statement ?name ?oids () =
  let finished, wakeup = Lwt.wait () in
  Connection.prepare
    conn
    ~statement
    ?name
    ?oids
    ~finish:(fun () -> Lwt.wakeup_later wakeup ())
    ();
  finished

let execute conn ?name ?statement ?parameters ~on_data_row () =
  let finished, wakeup = Lwt.wait () in
  Connection.execute conn ?name ?statement ?parameters ~on_data_row (fun () ->
      Lwt.wakeup_later wakeup ());
  finished
