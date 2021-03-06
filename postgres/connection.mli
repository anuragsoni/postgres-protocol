exception Auth_method_not_implemented of string

module User_info : sig
  type t =
    { user : string
    ; password : string
    ; database : string option
    }

  val make : user:string -> ?password:string -> ?database:string -> unit -> t
end

type error_handler = Error.t -> unit
type t
type driver = (module Runtime_intf.S with type t = t) -> t -> unit

module type IO = sig
  type 'a t

  val return : 'a -> 'a t
  val ( >>= ) : 'a t -> ('a -> 'b t) -> 'b t
  val of_cps : (error_handler -> ('a -> unit) -> unit) -> 'a t

  module Sequencer : sig
    type 'a future = 'a t
    type 'a t

    val create : 'a -> 'a t
    val enqueue : 'a t -> ('a -> 'b future) -> 'b future
  end
end

module type S = sig
  type 'a future
  type t

  val startup : driver -> User_info.t -> t future

  val prepare
    :  ?name:string
    -> ?oids:Types.Oid.t array
    -> statement:string
    -> t
    -> unit future

  val execute
    :  ?portal_name:string
    -> ?statement_name:string
    -> ?parameters:(Types.Format_code.t * string option) array
    -> (string option list -> unit)
    -> t
    -> unit future

  val close : Frontend.Close.t -> t -> unit future
  val terminate : t -> unit future
end

module Make (Io : IO) : S with type 'a future := 'a Io.t
