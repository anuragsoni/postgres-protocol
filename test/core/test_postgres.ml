open Postgres

let default_error_handler _ = failwith "BOOM"

module Util = struct
  let ready_for_query =
    let b = Bytes.create 6 in
    Bytes.set b 0 'Z';
    Bytes.set_int32_be b 1 5l;
    Bytes.set b 5 'I';
    Bytes.to_string b

  let write_op_to_list = function
    | `Close _ -> [ "CLOSE" ]
    | `Yield -> [ "YIELD" ]
    | `Write iovecs ->
      List.map
        (fun { Faraday.buffer; off; len } ->
          let b = Bigstringaf.sub buffer ~off ~len in
          Bigstringaf.to_string b)
        iovecs

  let pp_string_list fmt (l, r) =
    let p = Fmt.Dump.list Fmt.Dump.string in
    Format.fprintf fmt "Expected: %a Received: %a\n" p l p r

  let write_all conn =
    match Connection.next_write_operation conn with
    | `Close _ -> `Closed
    | `Yield -> `Ok 0
    | `Write iovecs ->
      let n = List.fold_left (fun acc { Faraday.len; _ } -> acc + len) 0 iovecs in
      `Ok n

  let string = Alcotest.testable Fmt.Dump.string String.equal

  let read_frames frames conn =
    let msgs =
      List.map (fun f -> Bigstringaf.of_string f ~off:0 ~len:(String.length f)) frames
    in
    List.iter
      (fun msg -> Connection.read conn msg ~off:0 ~len:(Bigstringaf.length msg) |> ignore)
      msgs
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
    "Receive md5 password"
    [ "p\000\000\000(md5290d3a28e45d0e2a0cb83cce2adb41e3\000" ]
    (Util.write_op_to_list (Connection.next_write_operation conn));
  let res = Util.write_all conn in
  Connection.report_write_result conn res;
  Alcotest.(check bool) "Log-in hasn't happened yet" false !auth_ok;
  let messages = [ "R\000\000\000\008\000\000\000\000"; Util.ready_for_query ] in
  Util.read_frames messages conn;
  Alcotest.(check bool) "Login successful" true !auth_ok

let tests =
  [ "encode startup message", `Quick, test_startup_payload
  ; "test_md5_auth", `Quick, test_md5_auth
  ]

let () = Alcotest.run "Postgres" [ "Protocol tests", tests ]
