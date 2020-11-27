module type S = sig
  type t

  val next_write_operation
    :  t
    -> [ `Close of int | `Write of Faraday.bigstring Faraday.iovec list | `Yield ]

  val report_write_result : t -> [ `Ok of int | `Closed ] -> unit
  val yield_writer : t -> (unit -> unit) -> unit
  val next_read_operation : t -> [ `Close | `Read ]
  val read : t -> Bigstringaf.t -> off:int -> len:int -> int
  val read_eof : t -> Bigstringaf.t -> off:int -> len:int -> int
  val yield_reader : t -> (unit -> unit) -> unit
  val shutdown : t -> unit
  val report_exn : t -> exn -> unit
end
