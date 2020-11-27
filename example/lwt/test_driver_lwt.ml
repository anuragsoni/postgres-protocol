open! Core

let make_parameters ids =
  Sequence.of_list ids
  |> Sequence.map ~f:(fun id ->
         let b = Bytes.create 4 in
         Caml.Bytes.set_int32_be b 0 id;
         `Binary, Some (Bytes.to_string b))
  |> Sequence.to_array
;;

let prepare_query name conn =
  Postgres_lwt.prepare
    ~name
    ~statement:"SELECT id, email from users where id IN ($1, $2, $3)"
    conn
;;

let run name conn ids =
  let parameters = make_parameters ids in
  (* If we use named prepared queries, we can reference them by name later on in the
     session lifecycle. *)
  Postgres_lwt.execute
    ~statement:name
    ~parameters
    (fun data_row ->
      match data_row with
      | [ Some id; Some name ] -> Logs.info (fun m -> m "Id: %s and email: %s" id name)
      | _ -> assert false)
    conn
;;

let execute conn =
  let open Lwt_result.Syntax in
  let name = "my_unique_query" in
  let* () = prepare_query name conn in
  let* () = run name conn [ 9l; 2l; 3l ]
  and* () = run name conn [ 2l; 4l; 10l ]
  and* () = run name conn [ 1l; 7l; 2l ]
  and* () = run name conn [ 78l; 11l; 6l ] in
  let+ () = Postgres_lwt.close conn in
  Logs.info (fun m -> m "Finished")
;;

let run host port user password database ssl =
  let open Lwt.Infix in
  let%bind user =
    match user with
    | None -> Lwt_unix.getlogin ()
    | Some u -> Lwt.return u
  in
  let tls_config =
    match ssl with
    | false -> None
    | true ->
      let authenticator ~host:_ _ = Ok None in
      Some (Tls.Config.client ~authenticator ())
  in
  let database = Option.value ~default:user database in
  let user_info = Postgres.Connection.User_info.make ~user ~password ~database () in
  let open Lwt_result.Infix in
  Postgres_lwt_unix.(connect ?tls_config user_info (Inet (host, port)))
  >>= fun conn -> execute conn
;;

let command =
  Command.basic
    ~summary:"Hello Postgres"
    Command.Param.(
      let open Command.Let_syntax in
      let%map port = flag "-port" (optional_with_default 5432 int) ~doc:"int PGPORT"
      and host =
        flag "-host" (optional_with_default "localhost" string) ~doc:"string PGHOST"
      and user = flag "-user" (optional string) ~doc:"string PGUSER"
      and password =
        flag "-password" (optional_with_default "" string) ~doc:"string PGPASSWORD"
      and database = flag "-database" (optional string) ~doc:"string PGDATABASE"
      and ssl = flag "-ssl" (optional_with_default false bool) ~doc:"bool SSL" in
      fun () ->
        Lwt_main.run
        @@
        let open Lwt.Infix in
        match%bind run host port user password database ssl with
        | Ok () -> Lwt.return ()
        | Error (`Exn exn) -> Lwt.fail exn
        | Error (`Msg msg) -> Lwt.fail_with msg)
;;

let () = Mirage_crypto_rng_unix.initialize ()

let () =
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level ~all:true (Some Info);
  Fmt_tty.setup_std_outputs ();
  Command.run command
;;
