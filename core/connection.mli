exception Auth_method_not_implemented of string

module User_info : sig
  type t =
    { user : string
    ; password : string
    ; database : string option
    }

  val make : user:string -> ?password:string -> ?database:string -> unit -> t
end

type error =
  [ `Exn of exn
  | `Msg of string
  | `Postgres_error of Backend.Error_response.t
  | `Parse_error of string
  ]

type error_handler = error -> unit
type t

val connect : User_info.t -> error_handler -> (unit -> unit) -> t

val prepare
  :  t
  -> statement:string
  -> ?name:string
  -> ?oids:Types.Oid.t array
  -> error_handler
  -> (unit -> unit)
  -> unit

val execute
  :  t
  -> ?name:string
  -> ?statement:string
  -> ?parameters:(Types.Format_code.t * string option) array
  -> (string option list -> unit)
  -> error_handler
  -> (unit -> unit)
  -> unit

val close : t -> unit

val next_write_operation
  :  t
  -> [> `Close of int | `Write of Faraday.bigstring Faraday.iovec list | `Yield ]

val next_read_operation : t -> [> `Close | `Read | `Yield ]
val read : t -> Angstrom.bigstring -> off:int -> len:int -> int
val read_eof : t -> Angstrom.bigstring -> off:int -> len:int -> int
val yield_reader : t -> (unit -> unit) -> unit
val report_write_result : t -> [< `Closed | `Ok of int ] -> unit
val report_exn : t -> exn -> unit
val yield_writer : t -> (unit -> unit) -> unit
