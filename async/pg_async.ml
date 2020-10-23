open Core
open Async
open Postgres
open Async_ssl

let ( >>|? ) = Deferred.Or_error.( >>| )
let src = Logs.Src.create "postgres.async"

module Log = (val Logs.src_log src : Logs.LOG)

type ssl_options =
  { version : Version.t option
  ; options : Opt.t list option
  ; name : string option
  ; hostname : string option
  ; allowed_ciphers : [ `Only of string list | `Openssl_default | `Secure ] option
  ; ca_file : string option
  ; ca_path : string option
  ; crt_file : string option
  ; key_file : string option
  ; verify_modes : Verify_mode.t list option
  ; session : (Ssl.Session.t[@sexp.opaque]) option
  ; verify_peer : Ssl.Connection.t -> unit Or_error.t
  }

module Destination = struct
  type t =
    | Inet of Host_and_port.t * ssl_options option
    | Unix_domain of string

  let of_inet ?ssl_options host_and_port = Inet (host_and_port, ssl_options)
  let of_file path = Unix_domain path
end

let create_ssl_options
    ?version
    ?options
    ?name
    ?hostname
    ?allowed_ciphers
    ?ca_file
    ?ca_path
    ?crt_file
    ?key_file
    ?verify_modes
    ?session
    ?(verify_peer = fun _ -> Or_error.return ())
    ()
  =
  { version
  ; options
  ; name
  ; hostname
  ; allowed_ciphers
  ; ca_file
  ; ca_path
  ; crt_file
  ; key_file
  ; verify_modes
  ; session
  ; verify_peer
  }

let create_ssl opts r w handler =
  let open Async_ssl in
  let net_to_ssl = Reader.pipe r in
  let ssl_to_net = Writer.pipe w in
  let app_to_ssl, app_writer = Pipe.create () in
  let app_reader, ssl_to_app = Pipe.create () in
  match%bind
    Ssl.client
      ?version:opts.version
      ?options:opts.options
      ?name:opts.name
      ?hostname:opts.hostname
      ?allowed_ciphers:opts.allowed_ciphers
      ?ca_file:opts.ca_file
      ?ca_path:opts.ca_path
      ?crt_file:opts.crt_file
      ?key_file:opts.key_file
      ?verify_modes:opts.verify_modes
      ?session:opts.session
      ~app_to_ssl
      ~ssl_to_app
      ~net_to_ssl
      ~ssl_to_net
      ()
  with
  | Error err -> Error.raise err
  | Ok conn ->
    let () = opts.verify_peer conn |> Or_error.ok_exn in
    let%bind reader =
      Reader.of_pipe (Info.of_string "postgres_async.ssl.reader") app_reader
    in
    let%bind writer, `Closed_and_flushed_downstream flush =
      Writer.of_pipe (Info.of_string "postgres_async.ssl.writer") app_writer
    in
    let shutdown () =
      let%bind () = Writer.close writer in
      let%bind () = flush in
      (* [Ssl.Connection.close] will cleanup and shutdown all the pipes provided to
         [Ssl.server]. *)
      Ssl.Connection.close conn;
      let%bind () =
        match%map Ssl.Connection.closed conn with
        | Ok _ -> ()
        | Error e -> Log.err (fun m -> m "%s" (Error.to_string_hum e))
      in
      Reader.close reader
    in
    Monitor.protect ~here:[%here] ~name:"postgres_async.ssl" ~finally:shutdown (fun () ->
        handler reader writer)

let request_ssl reader writer =
  let ssl_resp = Ivar.create () in
  let req = Request_ssl.create (fun r -> Ivar.fill_if_empty ssl_resp r) in
  let rec loop () =
    match Request_ssl.next_operation req with
    | `Write payload ->
      Writer.write_bytes writer payload;
      Request_ssl.report_write_result req (Bytes.length payload);
      loop ()
    | `Read ->
      (match%bind Reader.read_char reader with
      | `Eof -> failwith "Could not read postgres response"
      | `Ok c ->
        Request_ssl.feed_char req c;
        loop ())
    | `Stop -> return ()
    | `Fail msg -> failwith msg
  in
  don't_wait_for (loop ());
  Ivar.read ssl_resp

let fill_error ivar e =
  let err =
    match e with
    | `Exn e -> Error.of_exn e
    | `Msg msg -> Error.of_string msg
    | `Parse_error m -> Error.of_string (sprintf "Parse_error: %s" m)
    | `Postgres_error e -> Error.create_s ([%sexp_of: Backend.Error_response.t] e)
  in
  Ivar.fill_if_empty ivar (Error err)

let connect user_info destination =
  let connected = Ivar.create () in
  let conn =
    Postgres.Connection.connect
      user_info
      (fun e -> fill_error connected e)
      (fun () -> Ivar.fill_if_empty connected (Ok ()))
  in
  don't_wait_for
    (match destination with
    | Destination.Unix_domain path ->
      Tcp.with_connection (Tcp.Where_to_connect.of_file path) (fun _socket r w ->
          Io.run conn r w)
    | Inet (host_and_port, ssl_options) ->
      Tcp.with_connection
        (Tcp.Where_to_connect.of_host_and_port host_and_port)
        (fun _socket r w ->
          let handler = Io.run conn in
          match ssl_options with
          | None -> handler r w
          | Some ssl_options ->
            (match%bind request_ssl r w with
            | `Available -> create_ssl ssl_options r w handler
            | `Unavailable ->
              Error.raise_s [%message "Could not establish ssl connection"])));
  Ivar.read connected >>|? fun () -> conn

let prepare ~statement ?(name = "") ?(oids = [||]) conn =
  let ivar = Ivar.create () in
  Connection.prepare conn ~statement ~name ~oids (fill_error ivar) (fun () ->
      Ivar.fill_if_empty ivar (Ok ()));
  Ivar.read ivar

let execute ?(name = "") ?(statement = "") ?(parameters = [||]) on_data_row conn =
  let ivar = Ivar.create () in
  Connection.execute
    conn
    ~name
    ~statement
    ~parameters
    on_data_row
    (fill_error ivar)
    (fun () -> Ivar.fill_if_empty ivar (Ok ()));
  Ivar.read ivar

let close conn =
  Connection.close conn;
  Deferred.return (Ok ())
