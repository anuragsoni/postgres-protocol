open Postgres

let default_error_handler _ = failwith "BOOM"

module Util = struct
  let ready_for_query =
    let b = Bytes.create 6 in
    Bytes.set b 0 'Z';
    Bytes.set_int32_be b 1 5l;
    Bytes.set b 5 'I';
    Bytes.to_string b

  let read_op =
    let fmt fmt t =
      let m =
        match t with
        | `Read -> "Read"
        | `Yield -> "Yield"
        | `Close -> "Close"
      in
      Fmt.string fmt m
    in
    Alcotest.testable fmt (fun a b -> a = b)

  let write_op_to_list = function
    | `Close _ -> [ "CLOSE" ]
    | `Yield -> [ "YIELD" ]
    | `Write iovecs ->
      List.map
        (fun { Faraday.buffer; off; len } ->
          let b = Bigstringaf.sub buffer ~off ~len in
          Bigstringaf.to_string b)
        iovecs

  let write_all conn =
    match Connection.next_write_operation conn with
    | `Close _ -> `Closed
    | `Yield -> `Ok 0
    | `Write iovecs ->
      let n = List.fold_left (fun acc { Faraday.len; _ } -> acc + len) 0 iovecs in
      `Ok n

  let string = Alcotest.testable Fmt.Dump.string String.equal

  let exn =
    Alcotest.testable Fmt.exn (fun e1 e2 -> Printexc.to_string e1 = Printexc.to_string e2)

  let read_frames frames conn =
    let msgs =
      List.map (fun f -> Bigstringaf.of_string f ~off:0 ~len:(String.length f)) frames
    in
    List.iter
      (fun msg -> Connection.read conn msg ~off:0 ~len:(Bigstringaf.length msg) |> ignore)
      msgs

  let login () =
    let user_info = Connection.User_info.make ~user:"test" ~password:"password" () in
    let auth_ok = ref false in
    let conn =
      Connection.connect user_info default_error_handler (fun () -> auth_ok := true)
    in
    let res = write_all conn in
    Connection.report_write_result conn res;
    let message = [ "R\000\000\000\008\000\000\000\003" ] in
    read_frames message conn;
    let res = write_all conn in
    Connection.report_write_result conn res;
    let messages = [ "R\000\000\000\008\000\000\000\000"; ready_for_query ] in
    read_frames messages conn;
    Alcotest.(check bool) "Login successful" true !auth_ok;
    conn
end

let test_startup_payload () =
  let user_info = Connection.User_info.make ~user:"test" ~password:"password" () in
  let conn = Connection.connect user_info default_error_handler (fun () -> ()) in
  let next_write_op = Connection.next_write_operation conn in
  Alcotest.(check (list Util.string))
    "Can encode startup payload"
    [ "\000\000\000\019\000\003\000\000user\000test\000\000" ]
    (Util.write_op_to_list next_write_op);
  let user_info =
    Connection.User_info.make ~user:"test" ~password:"password" ~database:"mydatabase" ()
  in
  let conn = Connection.connect user_info default_error_handler (fun () -> ()) in
  let next_write_op = Connection.next_write_operation conn in
  Alcotest.(check (list Util.string))
    "Encode message with database name"
    [ "\000\000\000'\000\003\000\000user\000test\000database\000mydatabase\000\000" ]
    (Util.write_op_to_list next_write_op)

let test_md5_auth () =
  let user_info = Connection.User_info.make ~user:"test" ~password:"password" () in
  let auth_ok = ref false in
  let conn =
    Connection.connect user_info default_error_handler (fun () -> auth_ok := true)
  in
  let res = Util.write_all conn in
  Connection.report_write_result conn res;
  let message = [ "R\000\000\000\012\000\000\000\005gnqu" ] in
  Util.read_frames message conn;
  Alcotest.(check (list Util.string))
    "Prepare md5 password payload"
    [ "p\000\000\000(md5290d3a28e45d0e2a0cb83cce2adb41e3\000" ]
    (Util.write_op_to_list (Connection.next_write_operation conn));
  let res = Util.write_all conn in
  Connection.report_write_result conn res;
  Alcotest.(check bool) "Log-in hasn't happened yet" false !auth_ok;
  let messages = [ "R\000\000\000\008\000\000\000\000"; Util.ready_for_query ] in
  Util.read_frames messages conn;
  Alcotest.(check bool) "Login successful" true !auth_ok

let test_plain_auth () =
  let user_info = Connection.User_info.make ~user:"test" ~password:"password" () in
  let auth_ok = ref false in
  let conn =
    Connection.connect user_info default_error_handler (fun () -> auth_ok := true)
  in
  let res = Util.write_all conn in
  Connection.report_write_result conn res;
  let message = [ "R\000\000\000\008\000\000\000\003" ] in
  Util.read_frames message conn;
  Alcotest.(check (list Util.string))
    "Prepare plain text password payload"
    [ "p\000\000\000\rpassword\000" ]
    (Util.write_op_to_list (Connection.next_write_operation conn));
  let res = Util.write_all conn in
  Connection.report_write_result conn res;
  Alcotest.(check bool) "Log-in hasn't happened yet" false !auth_ok;
  let messages = [ "R\000\000\000\008\000\000\000\000"; Util.ready_for_query ] in
  Util.read_frames messages conn;
  Alcotest.(check bool) "Login successful" true !auth_ok

let test_prepare () =
  (* setup connection *)
  let conn = Util.login () in
  (* We will attempt to prepare two queries, but call them in a manner where the second
     query is scheduled before the first one has had a time to finish The query sequencer
     that's part of the core module should ensure that the two requests run sequentially. *)
  let prepared = Queue.create () in
  Connection.prepare
    conn
    ~statement:"SELECT * FROM USERS WHERE ID IN (?1, $2, $3)"
    default_error_handler
    (fun () -> Queue.push 1 prepared);
  let res = Util.write_all conn in
  Connection.report_write_result conn res;
  Alcotest.(check bool) "prepare transaction not finished" true (Queue.is_empty prepared);
  (* enqueue the second prepare request *)
  Connection.prepare
    conn
    ~statement:"SELECT * FROM USERS WHERE ID = $1"
    default_error_handler
    (fun () -> Queue.push 2 prepared);
  (* postgres operates on queries in sequence. The response for the first query arrives
     first. *)
  let messages = [ "1\000\000\000\004"; Util.ready_for_query ] in
  Util.read_frames messages conn;
  Alcotest.(check (list int))
    "First query finished"
    [ 1 ]
    (Queue.to_seq prepared |> List.of_seq);
  (* ensure that we are still looking to read more *)
  Alcotest.(check Util.read_op)
    "next read operation"
    `Read
    (Connection.next_read_operation conn);
  (* the response for the second query is similar since it was also a parse request *)
  Util.read_frames messages conn;
  Alcotest.(check (list int))
    "Second query finished"
    [ 1; 2 ]
    (Queue.to_seq prepared |> List.of_seq);
  (* we received responses for both queries, so next read operations is yield *)
  Alcotest.(check Util.read_op)
    "next read operation"
    `Yield
    (Connection.next_read_operation conn)

let test_fetch_rows_with_error () =
  (* setup connection *)
  let conn = Util.login () in
  let prepared = ref false in
  Connection.prepare
    conn
    ~statement:"SELECT * FROM USERS WHERE ID IN (?1, $2, $3)"
    default_error_handler
    (fun () -> prepared := true);
  let res = Util.write_all conn in
  Connection.report_write_result conn res;
  let messages = [ "1\000\000\000\004"; Util.ready_for_query ] in
  Util.read_frames messages conn;
  Alcotest.(check bool) "Query prepared" true !prepared;
  let error_seen = ref None in
  let error_handler e =
    match e with
    | `Exn e -> error_seen := Some e
    | _ -> ()
  in
  let rows_before_err = Queue.create () in
  let on_data_row rows =
    match rows with
    | Some "foo" :: _ -> failwith "BOOM"
    | Some x :: _ -> Queue.push x rows_before_err
    | _ -> failwith "invalid row"
  in
  let finished = ref false in
  let on_finish () = finished := true in
  Connection.execute conn error_handler on_data_row on_finish;
  let res = Util.write_all conn in
  Connection.report_write_result conn res;
  Alcotest.(check bool) "Execute not finished" false !finished;
  let data_rows =
    [ "D\000\000\000\020\000\002\000\000\000\003baz\000\000\000\003bar"
    ; "D\000\000\000\020\000\002\000\000\000\003foo\000\000\000\003bar"
    ; "D\000\000\000\020\000\002\000\000\000\003bba\000\000\000\003bar"
    ; Util.ready_for_query
    ]
  in
  Util.read_frames data_rows conn;
  Alcotest.(check (list string) "Rows before error")
    [ "baz" ]
    (Queue.to_seq rows_before_err |> List.of_seq);
  Alcotest.(check (option Util.exn)) "Check error" (Some (Failure "BOOM")) !error_seen;
  Alcotest.(check bool)
    "Error handler triggering means read-for-query not called"
    false
    !finished

let test_fetch_rows () =
  (* setup connection *)
  let conn = Util.login () in
  let prepared = ref false in
  Connection.prepare
    conn
    ~statement:"SELECT * FROM USERS WHERE ID IN (?1, $2, $3)"
    default_error_handler
    (fun () -> prepared := true);
  let res = Util.write_all conn in
  Connection.report_write_result conn res;
  let messages = [ "1\000\000\000\004"; Util.ready_for_query ] in
  Util.read_frames messages conn;
  Alcotest.(check bool) "Query prepared" true !prepared;
  let out_rows = Queue.create () in
  let on_data_row rows =
    match rows with
    | Some x :: Some y :: _ -> Queue.push (x, y) out_rows
    | _ -> failwith "invalid row"
  in
  let finished = ref false in
  let on_finish () = finished := true in
  Connection.execute conn default_error_handler on_data_row on_finish;
  let res = Util.write_all conn in
  Connection.report_write_result conn res;
  Alcotest.(check bool) "Execute not finished" false !finished;
  let data_rows =
    [ "D\000\000\000\020\000\002\000\000\000\003baz\000\000\000\003bar"
    ; "D\000\000\000\020\000\002\000\000\000\003foo\000\000\000\003bar"
    ; "D\000\000\000\020\000\002\000\000\000\003bba\000\000\000\003bar"
    ; Util.ready_for_query
    ]
  in
  Util.read_frames data_rows conn;
  Alcotest.(check (list (pair Util.string Util.string)) "Rows before error")
    [ "baz", "bar"; "foo", "bar"; "bba", "bar" ]
    (Queue.to_seq out_rows |> List.of_seq);
  Alcotest.(check bool) "Execute finished" true !finished

let tests =
  [ "startup", [ "encode startup message", `Quick, test_startup_payload ]
  ; ( "auth"
    , [ "md5 auth", `Quick, test_md5_auth; "plain text auth", `Quick, test_plain_auth ] )
  ; ( "execute"
    , [ "prepare queries", `Quick, test_prepare
      ; "exception in user handler", `Quick, test_fetch_rows_with_error
      ; "fetch rows", `Quick, test_fetch_rows
      ] )
  ]

let () = Alcotest.run "Postgres" tests
