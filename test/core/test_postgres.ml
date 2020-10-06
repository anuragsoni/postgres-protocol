open Postgres

let default_error_handler _ = failwith "BOOM"

module Util = struct
  let write_op_to_list = function
    | `Close _ -> [ "CLOSE" ]
    | `Yield -> [ "YIELD" ]
    | `Write iovecs ->
      List.map
        (fun { Faraday.buffer; off; len } ->
          let b = Bigstringaf.sub buffer ~off ~len in
          Bigstringaf.to_string b)
        iovecs

  let diff_string_pair fmt (l, r) = Format.fprintf fmt "Expected: %S, received: %S" l r
end

let test_startup_payload _ctxt =
  let user_info = Connection.User_info.make ~user:"test" ~password:"password" () in
  let conn = Connection.connect user_info default_error_handler (fun () -> ()) in
  let next_write_op = Connection.next_write_operation conn in
  OUnit2.assert_equal
    ~pp_diff:Util.diff_string_pair
    "\000\000\000\019\000\003\000\000user\000test\000\000"
    (Util.write_op_to_list next_write_op |> String.concat "\n");
  let user_info =
    Connection.User_info.make ~user:"test" ~password:"password" ~database:"mydatabase" ()
  in
  let conn = Connection.connect user_info default_error_handler (fun () -> ()) in
  let next_write_op = Connection.next_write_operation conn in
  OUnit2.assert_equal
    ~pp_diff:Util.diff_string_pair
    "\000\000\000'\000\003\000\000user\000test\000database\000mydatabase\000\000"
    (Util.write_op_to_list next_write_op |> String.concat "\n")

open OUnit2

let suite = "Postgres" >::: [ "test_startup_payload" >:: test_startup_payload ]
let () = run_test_tt_main suite
