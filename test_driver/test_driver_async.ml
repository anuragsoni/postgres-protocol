open! Core
open! Async

let create_socket host port =
  let where_to_connect =
    Tcp.Where_to_connect.of_host_and_port (Host_and_port.create ~host ~port)
  in
  let%map socket = Tcp.connect_sock where_to_connect in
  Ok socket

let run conn =
  let open Deferred.Or_error.Let_syntax in
  let%bind () =
    Pgasync.prepare ~statement:"SELECT id, email from users where id IN ($1, $2, $3)" conn
  in
  let make_id id =
    let b = Bytes.create 4 in
    Caml.Bytes.set_int32_be b 0 id;
    Postgres.Frontend.Bind.make_param ~parameter:(Bytes.to_string b) `Binary ()
  in
  Pgasync.execute
    ~parameters:[| make_id 2l; make_id 4l; make_id 9l |]
    (fun data_row ->
      match data_row with
      | [ id; name ] ->
        Log.Global.info
          !"Id: %{sexp: string option} and email: %{sexp: string option}"
          id
          name
      | _ -> assert false)
    conn

let connect socket user password =
  let user_info = Postgres.Connection.User_info.make ~user ~password () in
  Pgasync.connect
    (fun conn ->
      Deferred.ignore_m
        (Gluten_async.Client.create
           ~read_buffer_size:0x1000
           ~protocol:(module Postgres.Connection)
           conn
           socket))
    user_info

let main () =
  let open Deferred.Or_error.Let_syntax in
  let%bind socket = create_socket "localhost" 5432 in
  let%bind conn = connect socket "asoni" "password" in
  let%bind () = run conn
  and () = run conn in
  Deferred.map (Pgasync.close conn) ~f:Result.return

let () =
  Command.async_spec ~summary:"Sample client" Command.Spec.empty (fun () ->
      main () |> Deferred.Or_error.ok_exn)
  |> Command.run
