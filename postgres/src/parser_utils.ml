open Core

let take_till predicate (iobuf : (read, Iobuf.seek) Iobuf.t) =
  let rec loop count =
    if count >= Iobuf.length iobuf
    then Iobuf.Consume.stringo iobuf
    else (
      let ch = Iobuf.Peek.char iobuf ~pos:count in
      if predicate ch
      then (
        let v = Iobuf.Consume.stringo ~len:count iobuf in
        Iobuf.advance iobuf 1;
        v)
      else loop (count + 1))
  in
  loop 0
;;
