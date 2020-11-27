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
type runtime = (module Runtime_intf.S with type t = t)

val connect
  :  (runtime -> t -> unit)
  -> User_info.t
  -> error_handler
  -> (t -> unit)
  -> unit

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
