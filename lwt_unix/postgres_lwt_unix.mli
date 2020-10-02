val src : Logs.Src.t

type destination =
  | Unix_domain of string
  | Inet of string * int

val connect : destination -> Lwt_unix.file_descr Lwt.t
