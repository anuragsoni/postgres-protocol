open Lwt.Syntax

let make_parameters ids =
  List.to_seq ids
  |> Seq.map (fun id ->
         let b = Bytes.create 4 in
         Bytes.set_int32_be b 0 id;
         Postgres.Frontend.Bind.make_param ~parameter:(Bytes.to_string b) `Binary ())
  |> Array.of_seq

let prepare_query name conn =
  Postgres_lwt.prepare
    ~name
    ~statement:"SELECT id, email from users where id IN ($1, $2, $3)"
    conn

let run name conn ids =
  let parameters = make_parameters ids in
  (* If we use named prepared queries, we can reference them by name later on in the
     session lifecycle. *)
  Postgres_lwt.execute
    ~statement:name
    ~parameters
    (fun data_row ->
      match data_row with
      | [ id; name ] ->
        let pp_opt = Fmt.option Fmt.string in
        Logs.info (fun m -> m "Id: %a and email: %a" pp_opt id pp_opt name)
      | _ -> assert false)
    conn

let () = Mirage_crypto_rng_unix.initialize ()

let connect host port user password =
  let authenticator ~host:_ _ = Ok None in
  let tls_config = Tls.Config.client ~authenticator () in
  let user_info = Postgres.Connection.User_info.make ~user ~password () in
  Postgres_lwt_unix.(connect ~mode:(Tls tls_config) user_info (Inet (host, port)))

let () =
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level ~all:true (Some Info);
  Fmt_tty.setup_std_outputs ();
  Lwt_main.run
    (let* conn = connect "localhost" 5432 "asoni" "password" in
     let name = "my_unique_query" in
     let* () = prepare_query name conn in
     let* () = run name conn [ 9l; 2l; 3l ]
     and* () = run name conn [ 2l; 4l; 10l ]
     and* () = run name conn [ 1l; 7l; 5l ]
     and* () = run name conn [ 78l; 11l; 6l ] in
     let+ () = Postgres_lwt.close conn in
     Logs.info (fun m -> m "Finished"))
