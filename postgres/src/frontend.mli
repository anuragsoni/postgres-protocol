open Core

module Protocol_version : sig
  type t = V3_0 [@@deriving sexp_of]
end

module Message : sig
  type t =
    { ident : char option
    ; to_write : int
    ; write : (read_write, Iobuf.seek) Iobuf.t -> unit
    }

  val to_write : t -> int
  val write : t -> (read_write, Iobuf.seek) Iobuf.t -> unit
end

val startup
  :  ?protocol_version:Protocol_version.t
  -> ?database:string
  -> string
  -> Message.t

val password : string -> Message.t
val parse : ?name:string -> ?oids:int array -> string -> Message.t

val bind
  :  ?destination:string
  -> ?statement:string
  -> ?parameters:Param.t array
  -> ?result_formats:Format_code.t array
  -> unit
  -> Message.t

val execute : ?name:string -> [ `Count of int | `Unlimited ] -> Message.t
val sync : Message.t
val terminate : Message.t
val close_portal : string -> Message.t
val close_statement : string -> Message.t
val flush : Message.t

val sasl_initial_response
  :  ?channel_binding:Scram.Channel_binding.t
  -> string
  -> Message.t

val sasl_response : string -> Message.t
