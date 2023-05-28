open Core

(** [t] represents the format for a query parameter. As of 2023, postgresql supports two
    data formats, text and binary.

    {{: https://www.postgresql.org/docs/15/protocol-overview.html#PROTOCOL-FORMAT-CODES} https://www.postgresql.org/docs/15/protocol-overview.html#PROTOCOL-FORMAT-CODES} *)
type t =
  | Text
  | Binary
[@@deriving sexp_of, variants, compare, equal]

val of_int : int -> t Or_error.t
val to_int : t -> int

include Serializable_intf.S with type t := t
