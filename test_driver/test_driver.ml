open Lwt.Syntax

let create_socket host port =
  let* addresses =
    Lwt_unix.getaddrinfo host (Int.to_string port) [ Unix.(AI_FAMILY PF_INET) ]
  in
  let socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let+ () = Lwt_unix.connect socket (List.hd addresses).ai_addr in
  socket

let run conn =
  let* () =
    Postgres_lwt.prepare
      ~statement:"SELECT id, email from users where id IN ($1, $2, $3)"
      conn
  in
  let make_id id =
    let b = Bytes.create 4 in
    Bytes.set_int32_be b 0 id;
    Postgres.Frontend.Bind.make_param ~parameter:(Bytes.to_string b) `Binary ()
  in
  Postgres_lwt.execute
    ~parameters:[| make_id 2l; make_id 4l; make_id 9l |]
    (fun data_row ->
      match data_row with
      | [ id; name ] ->
        let pp_opt = Fmt.option Fmt.string in
        Logs.info (fun m -> m "Id: %a and email: %a" pp_opt id pp_opt name)
      | _ -> assert false)
    conn

let connect socket user password =
  let user_info = Postgres.Connection.User_info.make ~user ~password () in
  Postgres_lwt.connect
    (fun conn ->
      let+ _ =
        Gluten_lwt_unix.Client.create
          ~read_buffer_size:0x1000
          ~protocol:(module Postgres.Connection)
          conn
          socket
      in
      ())
    user_info

let () =
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level ~all:true (Some Info);
  Fmt_tty.setup_std_outputs ();
  Lwt_main.run
    (let* socket = create_socket "localhost" 5432 in
     let* conn = connect socket "asoni" "password" in
     let* () = run conn
     and* () = run conn in
     let+ () = Postgres_lwt.close conn in
     Logs.info (fun m -> m "Finished"))
