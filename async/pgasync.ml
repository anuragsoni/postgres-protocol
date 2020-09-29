open Core_kernel
open Async_kernel
open Postgres

type t = Connection.t Sequencer.t

let wakeup_err w err =
  let e =
    match err with
    | `Exn e -> Error.of_exn ~backtrace:`Get e
    | `Parse_error m -> Error.create_s [%message "Parse_error" ~message:m]
    | `Postgres_error { Backend.Error_response.message; _ } ->
      Error.create_s
        [%message
          "Postgres backend error" ~message:(Types.Optional_string.to_string message)]
  in
  Ivar.fill_if_empty w (Error e)

let connect run user_info =
  let ivar = Ivar.create () in
  let conn =
    Connection.connect user_info (wakeup_err ivar) (fun () ->
        Ivar.fill_if_empty ivar (Ok ()))
  in
  don't_wait_for (run conn);
  let%map res = Ivar.read ivar in
  Result.map res ~f:(fun () -> Sequencer.create ~continue_on_error:true conn)

let prepare ~statement ?(name = "") ?(oids = [||]) t =
  let ivar = Ivar.create () in
  Throttle.enqueue t
  @@ fun conn ->
  Connection.prepare conn ~statement ~name ~oids (wakeup_err ivar) (fun () ->
      Ivar.fill_if_empty ivar (Ok ()));
  Ivar.read ivar

let execute ?(name = "") ?(statement = "") ?(parameters = [||]) on_data_row t =
  let ivar = Ivar.create () in
  Throttle.enqueue t
  @@ fun conn ->
  Connection.execute
    conn
    ~name
    ~statement
    ~parameters
    (wakeup_err ivar)
    on_data_row
    (fun () -> Ivar.fill_if_empty ivar (Ok ()));
  Ivar.read ivar

let close t =
  Throttle.enqueue t
  @@ fun conn ->
  Connection.close conn;
  Deferred.unit
