open! Core

let write_cstr t buf =
  Iobuf.Fill.stringo buf t;
  Iobuf.Fill.uint8_trunc buf 0
;;

module Protocol_version = struct
  type t = V3_0 [@@deriving sexp]

  let to_write t =
    match t with
    | V3_0 -> 4
  ;;

  let write t buf =
    match t with
    | V3_0 ->
      Iobuf.Fill.int16_be_trunc buf 3;
      Iobuf.Fill.int16_be_trunc buf 0
  ;;
end

module Startup_message = struct
  let ident = None

  type t =
    { user : string
    ; database : string option
    ; protocol_version : Protocol_version.t
    }
  [@@deriving sexp]

  let create ?(protocol_version = Protocol_version.V3_0) ?database user =
    { user; database; protocol_version }
  ;;

  let user_key = "user"
  let database_key = "database"

  let to_write { user; database; protocol_version } =
    let user_len = String.length user_key + 1 + String.length user + 1 in
    let database_len =
      match database with
      | None -> 0
      | Some d -> String.length database_key + 1 + String.length d + 1
    in
    Protocol_version.to_write protocol_version + user_len + database_len + 1
  ;;

  let write t buf =
    Protocol_version.write t.protocol_version buf;
    write_cstr user_key buf;
    write_cstr t.user buf;
    Option.iter t.database ~f:(fun database ->
      write_cstr database_key buf;
      write_cstr database buf);
    Iobuf.Fill.uint8_trunc buf 0
  ;;
end

module Password_message = struct
  let ident = Some 'p'

  type t = Md5_or_plain of string [@@deriving sexp]

  let to_write (Md5_or_plain s) = String.length s + 1
  let write (Md5_or_plain s) buf = write_cstr s buf
end

module Sasl_initial_response = struct
  let ident = Some 'p'

  type t =
    { client_initial_message : string
    ; channel_binding : Scram.Channel_binding.t
    }

  let create client_initial_message channel_binding =
    { client_initial_message; channel_binding }
  ;;

  let to_write t =
    let mechanism_length =
      match t.channel_binding with
      | Scram.Channel_binding.Not_supported -> String.length "SCRAM-SHA-256"
    in
    mechanism_length + 1 + 4 + String.length t.client_initial_message
  ;;

  let write t buf =
    let mechanism =
      match t.channel_binding with
      | Scram.Channel_binding.Not_supported -> "SCRAM-SHA-256"
    in
    write_cstr mechanism buf;
    Iobuf.Fill.int32_be_trunc buf (String.length t.client_initial_message);
    Iobuf.Fill.stringo buf t.client_initial_message
  ;;
end

module Sasl_response = struct
  let ident = Some 'p'

  type t = string

  let to_write t = String.length t
  let write t buf = Iobuf.Fill.stringo buf t
end

module Parse = struct
  let ident = Some 'P'

  type t =
    { name : string
    ; statement : string
    ; oids : int Array.t
    }
  [@@deriving sexp]

  let to_write { name; statement; oids } =
    String.length name + 1 + String.length statement + 1 + 2 + (Array.length oids * 4)
  ;;

  let create ?(name = "") ?(oids = [||]) statement = { name; oids; statement }

  let write { name; statement; oids } buf =
    write_cstr name buf;
    write_cstr statement buf;
    Iobuf.Fill.uint16_be_trunc buf (Array.length oids);
    Array.iter oids ~f:(fun oid -> Iobuf.Fill.uint32_be_trunc buf oid)
  ;;
end

module Bind = struct
  let ident = Some 'B'

  type t =
    { destination : string
    ; statement : string
    ; parameters : Param.t Array.t
    ; result_formats : Format_code.t Array.t
    }
  [@@deriving sexp_of]

  let create
    ?(destination = "")
    ?(statement = "")
    ?(parameters = Array.of_list [])
    ?(result_formats = Array.of_list [])
    ()
    =
    { destination; statement; parameters; result_formats }
  ;;

  let to_write { destination; statement; parameters; result_formats } =
    let param_len = Array.length parameters in
    String.length destination
    + 1
    + String.length statement
    + 1
    + 2
    + (param_len * 2)
    + 2
    + Array.fold
        ~f:(fun acc { parameter; _ } ->
          acc + 4 + String.length (Option.value parameter ~default:""))
        ~init:0
        parameters
    + 2
    + (Array.length result_formats * 2)
  ;;

  let write { destination; statement; parameters; result_formats } buf =
    write_cstr destination buf;
    write_cstr statement buf;
    Iobuf.Fill.uint16_be_trunc buf (Array.length parameters);
    Array.iter parameters ~f:(fun { format_code; _ } ->
      Iobuf.Fill.uint16_be_trunc buf (Format_code.to_int format_code));
    Iobuf.Fill.uint16_be_trunc buf (Array.length parameters);
    Array.iter parameters ~f:(fun { parameter; _ } ->
      let len =
        match parameter with
        | None -> -1
        | Some v -> String.length v
      in
      Iobuf.Fill.uint32_be_trunc buf len;
      Option.iter parameter ~f:(Iobuf.Fill.stringo buf));
    Iobuf.Fill.uint16_be_trunc buf (Array.length result_formats);
    Array.iter result_formats ~f:(fun fmt ->
      Iobuf.Fill.uint16_be_trunc buf (Format_code.to_int fmt))
  ;;
end

module Execute = struct
  let ident = Some 'E'

  type t =
    { name : string
    ; max_rows : [ `Unlimited | `Count of int ]
    }
  [@@deriving sexp]

  let create ?(name = "") max_rows = { name; max_rows }
  let to_write { name; _ } = String.length name + 1 + 4

  let write { name; max_rows } buf =
    write_cstr name buf;
    let count =
      match max_rows with
      | `Unlimited -> 0
      | `Count p -> p
    in
    Iobuf.Fill.uint32_be_trunc buf count
  ;;
end

module Close = struct
  let ident = Some 'C'

  type t =
    { kind : Statement_or_portal.t
    ; name : string
    }
  [@@deriving sexp_of]

  let portal name = { kind = Portal; name }
  let statement name = { kind = Statement; name }
  let to_write { name; _ } = 1 + String.length name + 1

  let write { kind; name } buf =
    Iobuf.Fill.char buf (Statement_or_portal.to_char kind);
    write_cstr name buf
  ;;
end

let sync = 'S'
let terminate = 'X'
let flush = 'H'

module type Message_intf = sig
  type t

  val ident : char option
  val to_write : t -> int
  val write : t -> (read_write, Iobuf.seek) Iobuf.t -> unit
end

module Message = struct
  type t =
    { ident : char option
    ; to_write : int
    ; write : (read_write, Iobuf.seek) Iobuf.t -> unit
    }

  let to_write t = t.to_write
  let write t buf = t.write buf
end

let create_message (type a) (module M : Message_intf with type t = a) msg =
  { Message.ident = M.ident
  ; to_write = M.to_write msg
  ; write = (fun iobuf -> M.write msg iobuf)
  }
;;

let create_message_ident_only ident =
  { Message.ident = Some ident; to_write = 0; write = (fun _iobuf -> ()) }
;;

let startup ?protocol_version ?database user =
  let msg = Startup_message.create ?protocol_version ?database user in
  create_message (module Startup_message) msg
;;

let password msg =
  create_message (module Password_message) (Password_message.Md5_or_plain msg)
;;

let parse ?name ?oids statement =
  create_message (module Parse) (Parse.create ?name ?oids statement)
;;

let bind ?destination ?statement ?parameters ?result_formats () =
  create_message
    (module Bind)
    (Bind.create ?destination ?statement ?parameters ?result_formats ())
;;

let execute ?name count = create_message (module Execute) (Execute.create ?name count)
let sync = create_message_ident_only sync
let terminate = create_message_ident_only terminate
let close_portal name = create_message (module Close) (Close.portal name)
let close_statement name = create_message (module Close) (Close.statement name)
let flush = create_message_ident_only flush

let sasl_initial_response
  ?(channel_binding = Scram.Channel_binding.Not_supported)
  client_initial_message
  =
  let msg = Sasl_initial_response.create client_initial_message channel_binding in
  create_message (module Sasl_initial_response) msg
;;

let sasl_response response = create_message (module Sasl_response) response
