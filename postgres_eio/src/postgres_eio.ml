open! Core
open Eio.Std
open Postgres

type t =
  { reader : Eio.Buf_read.t
  ; writer : Eio.Buf_write.t
  ; mutex : Eio.Mutex.t
  ; switch : Eio.Switch.t
  ; closed : unit Promise.t * unit Promise.u
  }

let closed t = fst t.closed
let is_closed t = Promise.is_resolved (fst t.closed)

let write_message t (message : Frontend.Message.t) =
  let to_write (message : Frontend.Message.t) =
    match message.ident with
    | None -> message.to_write + 4
    | Some _ -> message.to_write + 5
  in
  Eio.Buf_write.write_gen
    ~len:(to_write message)
    ~off:0
    t.writer
    message
    ~blit:(fun (message : Frontend.Message.t) ~src_off:_ buf ~dst_off ~len ->
    let iobuf = Iobuf.of_bigstring buf ~pos:dst_off ~len in
    Option.iter message.ident ~f:(fun c -> Iobuf.Fill.char iobuf c);
    Iobuf.Fill.int32_be_trunc iobuf (message.to_write + 4);
    message.write iobuf)
;;

let rec read_message t =
  let view = Eio.Buf_read.peek t.reader in
  let iobuf = Iobuf.of_bigstring view.buffer ~pos:view.off ~len:view.len in
  match Backend.parse (Iobuf.read_only iobuf) with
  | Error (Backend.Not_enough_data_for_message_frame { need_at_least }) ->
    (match
       Fiber.first
         (fun () ->
           Eio.Buf_read.ensure t.reader need_at_least;
           Ok ())
         (fun () ->
           Promise.await (closed t);
           Or_error.error_string "Connection closed")
     with
     | Ok () -> read_message t
     | Error e -> Error e)
  | Error (Fail message) -> Or_error.error_string message
  | Error (Exn exn) -> Or_error.of_exn exn
  | Ok message ->
    let consumed = view.len - Iobuf.length iobuf in
    Eio.Buf_read.consume t.reader consumed;
    Ok message
;;

let negotiate_sasl conn ~password =
  let open Or_error.Let_syntax in
  let client_initial_message = Postgres.Scram.Client_first_message.create () in
  let client_initial_message' =
    Postgres.Scram.Client_first_message.message client_initial_message
  in
  let message = Postgres.Frontend.sasl_initial_response client_initial_message' in
  write_message conn message;
  let rec loop state =
    let%bind message = read_message conn in
    match message with
    | message ->
      (match message, state with
       | ( Postgres.Backend.Authentication
             (Postgres.Backend.Authentication.SASLContinue payload)
         , `Client_initial_message client_initial_message ) ->
         (match
            Postgres.Scram.Server_first_response.parse
              client_initial_message
              (Iobuf.of_string payload)
          with
          | None -> Or_error.error_string "Invalid server first response"
          | Some server_first_response ->
            let client_final_message, server_signature =
              Postgres.Scram.Server_first_response.client_proof_and_server_signature
                ~password
                server_first_response
            in
            let client_final_response =
              Postgres.Frontend.sasl_response client_final_message
            in
            write_message conn client_final_response;
            loop (`Verify_server_signature server_signature))
       | ( Postgres.Backend.Authentication
             (Postgres.Backend.Authentication.SASLFinal payload)
         , `Verify_server_signature server_signature ) ->
         if String.equal payload server_signature
         then Ok ()
         else Or_error.error_string "Server signature failed to match. Auth failed"
       | message, _ ->
         traceln
           "Unexpected message: %s"
           (Sexplib0.Sexp.to_string_hum (Backend.sexp_of_message message));
         Or_error.error_string "Unexpected message while negotiating SASL")
  in
  loop (`Client_initial_message client_initial_message)
;;

let login conn ~user ~password ~database =
  let open Or_error.Let_syntax in
  let startup_message = Frontend.startup ~database user in
  write_message conn startup_message;
  let rec loop () =
    let%bind message = read_message conn in
    match message with
    | Postgres.Backend.ErrorResponse error ->
      Or_error.errorf !"backend error: %{sexp: Backend.Error_response.t}" error
    | Postgres.Backend.NoticeResponse _error -> loop ()
    | Postgres.Backend.BackendKeyData _ -> loop ()
    | Authentication msg ->
      (match msg with
       | Backend.Authentication.AuthOk -> loop ()
       | Md5Password salt ->
         let hash = Postgres.Md5.hash ~username:user ~password ~salt in
         let password_message = Postgres.Frontend.password hash in
         write_message conn password_message;
         loop ()
       | KerberosV5 -> Or_error.error_string "Kerberos auth not implemented"
       | ClearTextPassword ->
         let password_message = Postgres.Frontend.password password in
         write_message conn password_message;
         loop ()
       | SCMCredential -> Or_error.error_string "SCMCredential not implemented"
       | GSS -> Or_error.error_string "GSS not implemented"
       | SSPI -> Or_error.error_string "SSPI not implemented"
       | GSSContinue _ -> Or_error.error_string "GSSContinue not implemented"
       | SASL _protocols ->
         (* TODO: validate that list of protocols says SCRAM-SHA-256 *)
         let%bind () = negotiate_sasl conn ~password in
         loop ()
       | m ->
         Or_error.errorf
           !"Unexpected message during auth negotiation: %{sexp: \
             Postgres.Backend.Authentication.t}"
           m)
    | ParameterStatus _status -> loop ()
    | ReadyForQuery _ -> Ok ()
    | m ->
      Or_error.errorf
        !"Unexpected message during startup: %{sexp: Postgres.Backend.message}"
        m
  in
  loop ()
;;

let close t =
  Eio.Mutex.use_rw ~protect:true t.mutex (fun () ->
    if Fn.non is_closed t then Promise.resolve (snd t.closed) ())
;;

let prepare ?name ?oids t statement =
  Eio.Mutex.use_rw ~protect:true t.mutex (fun () ->
    let prepare = Frontend.parse ?name ?oids statement in
    Eio.Buf_write.pause t.writer;
    write_message t prepare;
    write_message t Frontend.sync;
    Eio.Buf_write.unpause t.writer;
    Eio.Buf_write.flush t.writer;
    let open Or_error.Let_syntax in
    let rec loop () =
      let%bind message = read_message t in
      match message with
      | Backend.ParseComplete -> loop ()
      | ReadyForQuery _ -> Ok ()
      | NoticeResponse _ -> loop ()
      | ErrorResponse e ->
        Or_error.errorf !"backend error: %{sexp: Postgres.Backend.Error_response.t}" e
      | m ->
        Or_error.errorf
          !"Unexpected message seen during parse: %{sexp: Backend.message}"
          m
    in
    loop ())
;;

let execute ?destination ?statement ?parameters ?result_formats ~on_data_row t =
  Eio.Mutex.use_rw ~protect:true t.mutex (fun () ->
    let bind = Frontend.bind ?destination ?statement ?parameters ?result_formats () in
    let execute = Frontend.execute ?name:destination `Unlimited in
    Eio.Buf_write.pause t.writer;
    write_message t bind;
    write_message t execute;
    write_message t Frontend.sync;
    Eio.Buf_write.unpause t.writer;
    Eio.Buf_write.flush t.writer;
    let open Or_error.Let_syntax in
    let rec loop () =
      let%bind message = read_message t in
      match message with
      | Backend.BindComplete -> loop ()
      | CommandComplete _ -> loop ()
      | ErrorResponse e ->
        Or_error.errorf !"backend error: %{sexp: Backend.Error_response.t}" e
      | ReadyForQuery _ -> Ok ()
      | NoticeResponse _ -> loop ()
      | DataRow row ->
        (match on_data_row row with
         | () -> loop ()
         | exception exn -> Or_error.of_exn exn)
      | m ->
        Or_error.errorf
          !"Unexpected message seen during parse: %{sexp: Backend.message}"
          m
    in
    loop ())
;;

let connect ~sw ~net ~user ~password ~database addr =
  let open Eio in
  let flow = Net.connect ~sw net addr in
  let reader = Buf_read.of_flow flow ~max_size:Int.max_value in
  let writer_p, writer_r = Promise.create () in
  let closed = Promise.create () in
  Fiber.fork ~sw (fun () ->
    Buf_write.with_flow flow (fun writer ->
      Promise.resolve writer_r writer;
      Promise.await (fst closed)));
  let t =
    { reader
    ; writer = Promise.await writer_p
    ; closed
    ; mutex = Eio.Mutex.create ()
    ; switch = sw
    }
  in
  Switch.on_release t.switch (fun () -> close t);
  match login t ~user ~password ~database with
  | Ok () -> Ok t
  | Error e ->
    close t;
    Error e
;;
