open Core
open Eio.Std

let drop_users = "DROP TABLE IF EXISTS users;"

let create_user_table =
  {|
    CREATE TABLE IF NOT EXISTS users(
      id SERIAL PRIMARY KEY,
      email VARCHAR(40) NOT NULL UNIQUE
    );
  |}
;;

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
;;

let initialize_database conn =
  let open Or_error.Let_syntax in
  traceln "initialize_database";
  let%bind () = Postgres_eio.prepare conn drop_users in
  let%bind () = Postgres_eio.execute ~on_data_row:ignore conn in
  traceln "dropped users";
  let%bind () = Postgres_eio.prepare conn create_user_table in
  let%bind () = Postgres_eio.execute ~on_data_row:ignore conn in
  traceln "prepared create users table";
  traceln "created users table";
  let%bind () = Postgres_eio.prepare conn create_random_users in
  Postgres_eio.execute ~on_data_row:ignore conn
;;

let make_parameters ids =
  Sequence.of_list ids
  |> Sequence.map ~f:(fun id ->
       let b = Bytes.create 4 in
       Caml.Bytes.set_int32_be b 0 id;
       Postgres.Param.create ~parameter:(Bytes.to_string b) Postgres.Format_code.binary)
  |> Sequence.to_array
;;

let run statement_name conn ids =
  let parameters = make_parameters ids in
  Postgres_eio.execute
    conn
    ~statement:statement_name
    ~parameters
    ~on_data_row:(fun data_row ->
    match data_row with
    | [| Some id; Some name |] -> traceln "Id: %s and email: %s" id name
    | _ -> assert false)
;;

let run_query conn =
  let open Or_error.Let_syntax in
  let name = "test_query" in
  let%bind () =
    Postgres_eio.prepare conn ~name "SELECT id, email from users where id IN ($1, $2, $3)"
  in
  traceln "query bind successful";
  run name conn [ 9l; 2l; 3l ]
;;

let main ~sw ~net =
  let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, 5432) in
  let conn =
    Or_error.ok_exn
      (Postgres_eio.connect
         ~sw
         ~net
         ~user:"anuragsoni"
         ~password:"hunter2"
         ~database:"anuragsoni"
         addr)
  in
  traceln "Logged in to postgres";
  let%bind.Or_error () = initialize_database conn in
  Caml.Fun.protect ~finally:(fun () -> Postgres_eio.close conn) (fun () -> run_query conn)
;;

let () =
  Eio_main.run (fun env ->
    Mirage_crypto_rng_eio.run
      (module Mirage_crypto_rng.Fortuna)
      env
      (fun () ->
        Switch.run (fun sw -> Or_error.ok_exn (main ~sw ~net:(Eio.Stdenv.net env)))))
;;
