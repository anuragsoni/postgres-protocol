open Async_kernel
open Postgres

type t

val connect
  :  (Connection.t -> unit Deferred.t)
  -> Connection.User_info.t
  -> t Deferred.Or_error.t

val prepare
  :  statement:string
  -> ?name:string
  -> ?oids:Types.Oid.t array
  -> t
  -> unit Deferred.Or_error.t

val execute
  :  ?name:string
  -> ?statement:string
  -> ?parameters:Frontend.Bind.parameter array
  -> (string option list -> unit)
  -> t
  -> unit Deferred.Or_error.t

val close : t -> unit Deferred.t
