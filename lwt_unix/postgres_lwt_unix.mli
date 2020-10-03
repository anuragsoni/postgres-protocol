val src : Logs.Src.t

type mode =
  | Regular
  | Tls of Tls.Config.client

type destination =
  | Unix_domain of string
  | Inet of string * int

val connect
  :  ?mode:mode
  -> Postgres.Connection.User_info.t
  -> destination
  -> Postgres_lwt.t Lwt.t
