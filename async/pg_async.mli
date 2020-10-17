open! Core
open! Async
open Async_ssl
open Postgres

val src : Logs.src

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

module Destination : sig
  type t

  val of_inet : ?ssl_options:ssl_options -> Host_and_port.t -> t
  val of_file : string -> t
end

val create_ssl_options
  :  ?version:Version.t
  -> ?options:Opt.t list
  -> ?name:string
  -> ?hostname:string
  -> ?allowed_ciphers:[ `Only of string list | `Openssl_default | `Secure ]
  -> ?ca_file:string
  -> ?ca_path:string
  -> ?crt_file:string
  -> ?key_file:string
  -> ?verify_modes:Verify_mode.t list
  -> ?session:Ssl.Session.t
  -> ?verify_peer:(Ssl.Connection.t -> unit Or_error.t)
  -> unit
  -> ssl_options

val connect : Connection.User_info.t -> Destination.t -> Connection.t Deferred.Or_error.t

val prepare
  :  statement:string
  -> ?name:string
  -> ?oids:Types.Oid.t array
  -> Connection.t
  -> unit Deferred.Or_error.t

val execute
  :  ?name:string
  -> ?statement:string
  -> ?parameters:Frontend.Bind.parameter array
  -> (string option list -> unit)
  -> Connection.t
  -> unit Deferred.Or_error.t

val close : Postgres.Connection.t -> unit Deferred.Or_error.t
