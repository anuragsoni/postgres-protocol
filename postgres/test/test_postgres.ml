open Core

let ready_for_query =
  let b = Iobuf.create ~len:6 in
  Iobuf.Fill.char b 'Z';
  Iobuf.Fill.int32_be_trunc b 5;
  Iobuf.Fill.char b 'I';
  Iobuf.flip_lo b;
  Iobuf.to_string b
;;

let%expect_test "can parse frames" =
  let frames =
    [ "1\000\000\000\004"
    ; ready_for_query
    ; "R\000\000\000\012\000\000\000\005gnqu"
    ; "R\000\000\000\008\000\000\000\000"
    ; "D\000\000\000\020\000\002\000\000\000\003baz\000\000\000\003bar"
    ; "D\000\000\000\020\000\002\000\000\000\003foo\000\000\000\003bar"
    ; "D\000\000\000\020\000\002\000\000\000\003bba\000\000\000\003bar"
    ]
  in
  let len = List.fold frames ~init:0 ~f:(fun acc elem -> acc + String.length elem) in
  let iobuf = Iobuf.create ~len in
  List.iter frames ~f:(Iobuf.Fill.stringo iobuf);
  Iobuf.flip_lo iobuf;
  let rec loop () =
    if Iobuf.is_empty iobuf
    then Ok []
    else (
      let%bind.Result message = Postgres.Backend.parse (Iobuf.read_only iobuf) in
      let x = Postgres.Backend.sexp_of_message message in
      let%map.Result xs = loop () in
      x :: xs)
  in
  match loop () with
  | Error e ->
    print_endline (Sexp.to_string_hum ~indent:2 (Postgres.Backend.sexp_of_error e))
  | Ok payloads ->
    List.iter payloads ~f:(fun sexp -> print_endline (Sexp.to_string_hum ~indent:2 sexp));
    [%expect
      {|
    ParseComplete
    (ReadyForQuery Idle)
    (Authentication (Md5Password gnqu))
    (Authentication AuthOk)
    (DataRow ((baz) (bar)))
    (DataRow ((foo) (bar)))
    (DataRow ((bba) (bar))) |}]
;;
