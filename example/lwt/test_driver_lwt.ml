open Lwt.Syntax

let make_parameters ids =
  List.to_seq ids
  |> Seq.map (fun id ->
         let b = Bytes.create 4 in
         Caml.Bytes.set_int32_be b 0 id;
         `Binary, Some (Bytes.to_string b))
  |> Array.of_seq
;;

let prepare_query name conn =
  Postgres_lwt.prepare
    ~name
    ~statement:"SELECT id, email from users where id IN ($1, $2, $3)"
    conn
;;

let setup conn =
  let open Lwt_result.Infix in
  let drop_users = "DROP TABLE IF EXISTS users;" in
  let create_users =
    {|
    CREATE TABLE IF NOT EXISTS users(
      id SERIAL PRIMARY KEY,
      email VARCHAR(40) NOT NULL UNIQUE
    );
  |}
  in
  let create_random_users =
    {|
    INSERT INTO users(email)
    SELECT
    'user_' || seq || '@' || (
      CASE (RANDOM() * 2)::INT
        WHEN 0 THEN 'gmail'
        WHEN 1 THEN 'hotmail'
        WHEN 2 THEN 'yahoo'
      END
    ) || '.com' AS email
    FROM GENERATE_SERIES(1, 10) seq;
  |}
  in
  Postgres_lwt.prepare ~statement:drop_users conn
  >>= fun () ->
  Postgres_lwt.execute (fun _ -> ()) conn
  >>= fun () ->
  Postgres_lwt.prepare ~statement:create_users conn
  >>= fun () ->
  Postgres_lwt.execute (fun _ -> ()) conn
  >>= fun () ->
  Postgres_lwt.prepare ~statement:create_random_users conn
  >>= fun () -> Postgres_lwt.execute (fun _ -> ()) conn
;;

let run statement_name conn ids =
  let parameters = make_parameters ids in
  (* If we use named prepared queries, we can reference them by name later on in the
     session lifecycle. *)
  Postgres_lwt.execute
    ~statement_name
    ~parameters
    (fun data_row ->
      match data_row with
      | [ Some id; Some name ] -> Logs.info (fun m -> m "Id: %s and email: %s" id name)
      | _ -> assert false)
    conn
;;

let execute conn =
  let open Lwt_result.Syntax in
  let* () = setup conn in
  let name = "my_unique_query" in
  let* () = prepare_query name conn in
  let* () = run name conn [ 9l; 2l; 3l ]
  and* () = run name conn [ 2l; 4l; 10l ]
  and* () = run name conn [ 1l; 7l; 2l ]
  and* () = run name conn [ 78l; 11l; 6l ] in
  let+ () = Postgres_lwt.terminate conn in
  Logs.info (fun m -> m "Finished")
;;

let run host port user password database ssl =
  let* user =
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

let cmd =
  let open Cmdliner in
  let port =
    let doc = "port number for the postgres server" in
    Arg.(value & opt int 5432 & info [ "p"; "port" ] ~doc)
  in
  let host =
    let doc = "hostname for the postgres server" in
    Arg.(value & opt string "localhost" & info [ "host" ] ~doc)
  in
  let user =
    let doc = "postgres user" in
    Arg.(value & opt (some string) None & info [ "user" ] ~doc)
  in
  let password =
    let doc = "postgres password" in
    Arg.(value & opt string "" & info [ "password" ] ~doc)
  in
  let database =
    let doc = "postgres database" in
    Arg.(value & opt (some string) None & info [ "database" ] ~doc)
  in
  let ssl =
    let doc = "setup a tls encrypted connection" in
    Arg.(value & flag & info [ "ssl" ] ~doc)
  in
  let doc = "Postgres example" in
  let term = Term.(const run $ host $ port $ user $ password $ database $ ssl) in
  term, Term.info "postgres_lwt" ~doc
;;

let run_cmd cmd =
  let open Cmdliner in
  let open Lwt.Infix in
  match Term.eval cmd with
  | `Ok res ->
    let p =
      res
      >>= function
      | Ok () -> Lwt.return ()
      | Error err -> Postgres.Error.raise err
    in
    Lwt_main.run p
  | _ -> exit 1
;;

let () = Mirage_crypto_rng_unix.initialize ()

let () =
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level ~all:true (Some Info);
  Fmt_tty.setup_std_outputs ();
  run_cmd cmd
;;
