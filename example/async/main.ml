open! Core
open! Async
open Postgres

let make_parameters ids =
  Sequence.of_list ids
  |> Sequence.map ~f:(fun id ->
         let b = Bytes.create 4 in
         Caml.Bytes.set_int32_be b 0 id;
         Postgres.Frontend.Bind.make_param ~parameter:(Bytes.to_string b) `Binary ())
  |> Sequence.to_array

let prepare_query name conn =
  Pg_async.prepare
    ~name
    ~statement:"SELECT id, email from users where id IN ($1, $2, $3)"
    conn

let execute name conn ids =
  let parameters = make_parameters ids in
  Pg_async.execute
    ~statement:name
    ~parameters
    (fun data_row ->
      match data_row with
      | [ Some id; Some name ] -> Logs.info (fun m -> m "Id: %s and email: %s" id name)
      | _ -> assert false)
    conn

let run host port user password database ssl =
  let%bind user =
    match user with
    | None -> Unix.getlogin ()
    | Some u -> return u
  in
  let ssl_options =
    match ssl with
    | false -> None
    | true ->
      Some
        (Pg_async.create_ssl_options
           ~verify_modes:[ Async_ssl.Verify_mode.Verify_none ]
           ())
  in
  let database = Option.value database ~default:user in
  let host_and_port = Host_and_port.create ~host ~port in
  let destination = Pg_async.Destination.of_inet ?ssl_options host_and_port in
  let user_info = Connection.User_info.make ~user ?password ~database () in
  let open Deferred.Or_error.Let_syntax in
  let%bind conn = Pg_async.connect user_info destination in
  let name = "my_unique_query" in
  let%bind () = prepare_query name conn in
  let%bind () = execute name conn [ 9l; 2l; 3l ]
  and () = execute name conn [ 2l; 4l; 10l ]
  and () = execute name conn [ 1l; 7l; 5l ]
  and () = execute name conn [ 78l; 11l; 6l ] in
  let%map () = Pg_async.close conn in
  Logs.info (fun m -> m "Finished")

let command =
  Command.async
    ~summary:"Hello Postgres"
    Command.Param.(
      let open Command.Let_syntax in
      let%map port = flag "-port" (optional_with_default 5432 int) ~doc:"int PGPORT"
      and host =
        flag "-host" (optional_with_default "localhost" string) ~doc:"string PGHOST"
      and user = flag "-user" (optional string) ~doc:"string PGUSER"
      and password = flag "-password" (optional string) ~doc:"string PGPASSWORD"
      and database = flag "-database" (optional string) ~doc:"string PGDATABASE"
      and ssl = flag "-ssl" (optional_with_default false bool) ~doc:"bool SSL" in
      fun () -> run host port user password database ssl |> Deferred.Or_error.ok_exn)

let () =
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level ~all:true (Some Info);
  Fmt_tty.setup_std_outputs ();
  Command.run command
