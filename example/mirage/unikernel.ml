module Make
    (STACK : Mirage_stack.V4)
    (TIME : Mirage_time.S)
    (RANDOM : Mirage_random.S)
    (MCLOCK : Mirage_clock.MCLOCK)
    (PCLOCK : Mirage_clock.PCLOCK) =
struct
  module Logs_reporter = Mirage_logs.Make (PCLOCK)
  module PG = Postgres_mirage.Make (STACK) (TIME) (MCLOCK) (RANDOM)

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

  let run statement_name conn ids =
    let parameters = make_parameters ids in
    (* If we use named prepared queries, we can reference them by name later on in the
       session lifecycle. *)
    Postgres_lwt.execute
      ~statement_name
      ~parameters
      (fun data_row ->
        match data_row with
        | [ id; name ] ->
          let pp_opt = Fmt.option Fmt.string in
          Logs.info (fun m -> m "Id: %a and email: %a" pp_opt id pp_opt name)
        | _ -> assert false)
      conn
  ;;

  let connect stack user password host port =
    let authenticator ~host:_ _ = Ok None in
    let tls_config = Tls.Config.client ~authenticator () in
    let domain = Domain_name.of_string_exn host |> Domain_name.host_exn in
    let user_info = Postgres.Connection.User_info.make ~user ~password () in
    PG.create stack ~tls_config user_info Postgres_mirage.(Domain (domain, port))
  ;;

  let start stack _time _random _mclock pclock =
    Logs.(set_level (Some Info));
    Logs_reporter.(create () |> run)
    @@ fun () ->
    Logs.info (fun m -> m "Hello from unikernel");
    let port = Key_gen.pgport () in
    let host = Key_gen.pghost () in
    let user = Key_gen.pguser () in
    let password = Key_gen.pgpassword () in
    let database = Key_gen.pgdatabase () in
    let res =
      let open Lwt_result.Syntax in
      let* conn = connect stack user password host port in
      let name = "my_unique_query" in
      let* () = prepare_query name conn in
      let* () = run name conn [ 9l; 2l; 3l ]
      and* () = run name conn [ 2l; 4l; 10l ]
      and* () = run name conn [ 1l; 7l; 2l ]
      and* () = run name conn [ 78l; 11l; 6l ] in
      let+ () = Postgres_lwt.terminate conn in
      Logs.info (fun m -> m "Finished")
    in
    let open Lwt.Infix in
    res
    >>= function
    | Ok () -> Lwt.return ()
    | Error err -> Postgres.Connection.Error.raise err
  ;;
end
