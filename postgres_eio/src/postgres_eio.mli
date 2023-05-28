open Core
open Eio.Std

type t

val closed : t -> unit Promise.t
val is_closed : t -> bool
val close : t -> unit
val prepare : ?name:string -> ?oids:int array -> t -> string -> unit Or_error.t

val execute
  :  ?destination:string
  -> ?statement:string
  -> ?parameters:Postgres.Frontend.Param.t array
  -> ?result_formats:Postgres.Format_code.t array
  -> on_data_row:(Postgres.Backend.Data_row.t -> unit)
  -> t
  -> unit Or_error.t

val connect
  :  sw:Eio.Switch.t
  -> net:#Eio.Net.t
  -> user:string
  -> password:string
  -> database:string
  -> Eio.Net.Sockaddr.stream
  -> t Or_error.t
