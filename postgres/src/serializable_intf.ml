open Core

module type S = sig
  type t [@@deriving sexp_of]

  (** [to_write] indicates the amount of space that needs to be available in the Iobuf,
      before writing [t]. Users should call this before attempting to write the value to
      an Iobuf, to ensure enough space has been made available in the Iobuf. *)
  val to_write : t -> int

  val write : t -> (read_write, Iobuf.seek) Iobuf.t -> unit
end
