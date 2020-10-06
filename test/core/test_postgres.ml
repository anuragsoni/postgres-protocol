open Postgres

let default_error_handler _ = failwith "BOOM"

module Util = struct
  let write_op_to_list = function
    | `Close _ -> [ "CLOSE" |> Bytes.of_string ]
    | `Yield -> [ "YIELD" |> Bytes.of_string ]
    | `Write iovecs ->
      List.map
        (fun { Faraday.buffer; off; len } ->
          let b = Bigstringaf.sub buffer ~off ~len in
          Bigstringaf.to_string b |> Bytes.of_string)
        iovecs

  let pp_string_list fmt (l, r) =
    let p = Fmt.Dump.list Fmt.Dump.string in
    Format.fprintf fmt "Expected: %a Received: %a\n" p l p r
end

let test_startup_payload () =
  let user_info = Connection.User_info.make ~user:"test" ~password:"password" () in
  let conn = Connection.connect user_info default_error_handler (fun () -> ()) in
  let next_write_op = Connection.next_write_operation conn in
  Alcotest.(check (list bytes))
    "Can encode startup payload"
    [ "\000\000\000\019\000\003\000\000user\000test\000\000" |> Bytes.of_string ]
    (Util.write_op_to_list next_write_op);
  let user_info =
    Connection.User_info.make ~user:"test" ~password:"password" ~database:"mydatabase" ()
  in
  let conn = Connection.connect user_info default_error_handler (fun () -> ()) in
  let next_write_op = Connection.next_write_operation conn in
  Alcotest.(check (list bytes))
    "Encode message with database name"
    [ "\000\000\000'\000\003\000\000user\000test\000database\000mydatabase\000\000"
      |> Bytes.of_string
    ]
    (Util.write_op_to_list next_write_op)

let tests = [ "encode startup message", `Quick, test_startup_payload ]
let () = Alcotest.run "Postgres" [ "Protocol tests", tests ]
