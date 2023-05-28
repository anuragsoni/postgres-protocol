open Core

module Channel_binding : sig
  type t = Not_supported

  val to_gs2_header : t -> string
end

module Client_first_message : sig
  type t =
    { nonce : string
    ; channel_binding : Channel_binding.t
    }

  val create : unit -> t
  val message : t -> string
  val message_bare : t -> string
end

module Server_first_response : sig
  type t =
    { client_first_message : Client_first_message.t
    ; payload : string
    ; salt : string
    ; iterations : int
    ; raw_message : string
    }

  val parse : Client_first_message.t -> (read, Iobuf.seek) Iobuf.t -> t option
  val client_proof_and_server_signature : password:string -> t -> string * string
end
