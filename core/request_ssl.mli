type t

val create : ([ `Available | `Unavailable ] -> unit) -> t
val next_operation : t -> [ `Write of bytes | `Read | `Fail of string | `Stop ]
val report_write_result : t -> int -> unit
val feed_char : t -> char -> unit
