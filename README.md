Work-in-progress IO agnostic postgres client. Supports connections over tcp (mirage + lwt.unix) and unix_domain_sockets (lwt.unix only).
Tcp connections also support encrypting postgres sessions over an ssl connection.
```ocaml
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
   via the functions in the `Postgres_lwt` module.

   Connections can either be un-encrypted (which is the default),
   or use ssl if the postgres server was compiled with SSL support
   enabled. Note: if setting the mode to TLS, you'd need to use
   Inet destination instead of Unix_domain. *)
let () = Mirage_crypto_rng_unix.initialize ()

let connect host port user password =
  let authenticator ~host:_ _ = Ok None in
  let tls_config = Tls.Config.client ~authenticator () in
  let user_info = Postgres.Connection.User_info.make ~user ~password () in
  Postgres_lwt_unix.(connect ~mode:(Tls tls_config) user_info (Inet (host, port)))

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

* add functions that'll clean up by closing connections after finishing a task
* support binary format for parameters
* Support additional auth methods (only MD5 and cleartext is implemented at the moment)
* Add parser/serializer for all postgres backend/frontend messages
* add wrapper to handle parameters in an easier manner
* documentation

Notes:

Documentation used for this implementation: https://www.postgresql.org/docs/12/protocol.html

