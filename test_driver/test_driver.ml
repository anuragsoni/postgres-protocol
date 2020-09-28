open Lwt.Syntax
module Postgres = Postgres_protocol_lwt_unix

let error_handler e =
  match e with
  | `Exn exn -> Logs.err (fun m -> m "%s" (Printexc.to_string exn))
  | `Parse_error msg -> Logs.err (fun m -> m "Parse error: %S" msg)
  | `Postgres_error e ->
    Logs.err (fun m ->
        m
          "postgres_error: %S"
          (e.Postgres_protocol.Backend.Error_response.message
          |> Postgres_protocol.Types.Optional_string.to_string))

let run conn =
  let* () =
    Postgres.prepare
      ~statement:"SELECT id, email from users where id IN ($1, $2, $3)"
      conn
      ()
  in
  let make_id id =
    let b = Bytes.create 4 in
    Bytes.set_int32_be b 0 id;
    Postgres_protocol.Frontend.Bind.make_param ~parameter:(Bytes.to_string b) `Binary ()
  in
  Postgres.execute
    conn
    ~parameters:[| make_id 2l; make_id 4l; make_id 9l |]
    ~on_data_row:(fun data_row ->
      match data_row with
      | [ id; name ] ->
        let pp_opt = Fmt.option Fmt.string in
        Logs.info (fun m -> m "Id: %a and email: %a" pp_opt id pp_opt name)
      | _ -> assert false)
    ()

let connect () =
  Postgres.connect
    ~host:"localhost"
    ~port:5432
    ~user:"asoni"
    ~password:"password"
    ~error_handler
    ()

let () =
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level ~all:true (Some Info);
  Fmt_tty.setup_std_outputs ();
  Lwt_main.run
    (let* conn = connect () in
     let+ () = run conn
     and+ () = run conn in
     ())
