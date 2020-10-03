Work-in-progress IO agnostic postgres client
```ocaml
open Lwt.Syntax

(* This example uses a helper utility from postgres-lwt-unix
   that simplifies the initial connection setup. Users can provide
   there own connection runners by providing a function
   `Connection.t -> unit` that drives the protocol state machine.
   https://github.com/anmonteiro/gluten is an example of one such
   runner. To avoid postgres-lwt-unix, provide the run function
   to `Postgres_lwt.connect`.

   Postgres_lwt_unix allows for creating connections via inet or
   unix domain sockets. Once a socket connection is established, it
   returns a `Postgres_lwt.t`. All further operations should be done
   via the functions in the `Postgres_lwt` module. *)
let connect host port user password =
  let user_info = Postgres.Connection.User_info.make ~user ~password () in
  Postgres_lwt_unix.(connect user_info (Inet (host, port)))

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
```

Incomplete TODO list:

* Tests (Lots of tests)
* better error handling
* add functions that'll clean up by closing connections after finishing a task
* support binary format for parameters
* Support additional auth methods (only MD5 and cleartext is implemented at the moment)
* ~~Support SSL/TLS connections~~ (we only support ocaml-tls at the moment)
* Add parser/serializer for all postgres backend/frontend messages
* add wrapper to handle parameters in an easier manner
* documentation

Notes:

Documentation used for this implementation: https://www.postgresql.org/docs/12/protocol.html

