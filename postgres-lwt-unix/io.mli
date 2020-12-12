module Socket : sig
  type t =
    | Regular of Lwt_unix.file_descr
    | Tls of Tls_lwt.Unix.t
end

val run : Socket.t -> Postgres.Connection.driver
