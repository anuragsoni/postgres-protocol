open Postgres
open Lwt.Syntax

module Throttle = struct
  type 'a t = 'a * Lwt_mutex.t

  let create conn = conn, Lwt_mutex.create ()
  let enqueue (conn, mutex) run = Lwt_mutex.with_lock mutex (fun () -> run conn)
end

type t = Connection.t Throttle.t

exception Parse_error of string
exception Postgres_error of Backend.Error_response.t

let wakeup_exn w err =
  let exn =
    match err with
    | `Exn e -> e
    | `Parse_error m -> Parse_error m
    | `Postgres_error e -> Postgres_error e
  in
  Lwt.wakeup_later_exn w exn

let connect run user_info =
  let finished, wakeup = Lwt.wait () in
  let conn = Connection.connect user_info (wakeup_exn wakeup) (Lwt.wakeup_later wakeup) in
  Lwt.async (fun () -> run conn);
  let+ () = finished in
  Throttle.create conn

let prepare ~statement ?(name = "") ?(oids = [||]) t =
  let finished, wakeup = Lwt.wait () in
  Throttle.enqueue t
  @@ fun conn ->
  Connection.prepare
    conn
    ~statement
    ~name
    ~oids
    (wakeup_exn wakeup)
    (Lwt.wakeup_later wakeup);
  finished

let execute ?(name = "") ?(statement = "") ?(parameters = [||]) on_data_row t =
  let finished, wakeup = Lwt.wait () in
  Throttle.enqueue t
  @@ fun conn ->
  Connection.execute
    conn
    ~name
    ~statement
    ~parameters
    (wakeup_exn wakeup)
    on_data_row
    (Lwt.wakeup_later wakeup);
  finished
