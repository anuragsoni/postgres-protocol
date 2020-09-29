open Postgres

type t

exception Parse_error of string
exception Postgres_error of Backend.Error_response.t

val connect : (Connection.t -> unit Lwt.t) -> Connection.User_info.t -> t Lwt.t

val prepare
  :  statement:string
  -> ?name:string
  -> ?oids:Types.Oid.t array
  -> t
  -> unit Lwt.t

val execute
  :  ?name:string
  -> ?statement:string
  -> ?parameters:Frontend.Bind.parameter array
  -> (string option list -> unit)
  -> t
  -> unit Lwt.t

val close : t -> unit Lwt.t
