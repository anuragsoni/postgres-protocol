open Core

type error =
  | Not_enough_data_for_message_frame of { need_at_least : int }
  | Fail of string
  | Exn of exn
[@@deriving sexp_of]

module Authentication : sig
  type t =
    | AuthOk
    | KerberosV5
    | ClearTextPassword
    | Md5Password of string
    | SCMCredential
    | GSS
    | GSSContinue of string
    | SSPI
    | SASL of string
    | SASLContinue of string
    | SASLFinal of string
  [@@deriving sexp_of]
end

module Ready_for_query : sig
  type t =
    | Idle
    | Transaction_block
    | Failed_transaction
  [@@deriving sexp_of]
end

module Backend_key_data : sig
  type t =
    { pid : int
    ; secret : int
    }
  [@@deriving sexp_of]
end

module Error_response : sig
  type kind =
    | Severity
    | Non_localized_severity
    | Code
    | Message
    | Detail
    | Hint
    | Position
    | Internal_position
    | Internal_query
    | Where
    | Schema_name
    | Table_name
    | Column_name
    | Datatype_name
    | Constraint_name
    | File
    | Line
    | Routine
    | Unknown of char
  [@@deriving sexp_of]

  type error_or_notice =
    { kind : kind
    ; message : string
    }
  [@@deriving sexp_of]

  type t = error_or_notice list [@@deriving sexp_of]
end

module Notice_response : sig
  type kind =
    | Severity
    | Non_localized_severity
    | Code
    | Message
    | Detail
    | Hint
    | Position
    | Internal_position
    | Internal_query
    | Where
    | Schema_name
    | Table_name
    | Column_name
    | Datatype_name
    | Constraint_name
    | File
    | Line
    | Routine
    | Unknown of char
  [@@deriving sexp_of]

  type error_or_notice =
    { kind : kind
    ; message : string
    }
  [@@deriving sexp_of]

  type t = error_or_notice list [@@deriving sexp_of]
end

module Parameter_status : sig
  type t =
    { name : string
    ; value : string
    }
  [@@deriving sexp_of]
end

module Data_row : sig
  type t = string option array [@@deriving sexp_of]
end

type message =
  | Authentication of Authentication.t
  | BackendKeyData of Backend_key_data.t
  | ReadyForQuery of Ready_for_query.t
  | BindComplete
  | ErrorResponse of Error_response.t
  | NoticeResponse of Notice_response.t
  | CloseComplete
  | ParseComplete
  | ParameterStatus of Parameter_status.t
  | DataRow of Data_row.t
  | CommandComplete of string
[@@deriving sexp_of]

val parse : (read, Iobuf.seek) Iobuf.t -> (message, error) Result.t
