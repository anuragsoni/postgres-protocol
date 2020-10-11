val src : Logs.Src.t

type destination =
  | Unix_domain of string
  | Inet of string * int

val connect
  :  ?tls_config:Tls.Config.client
  -> Postgres.Connection.User_info.t
  -> destination
  -> Postgres_lwt.t Lwt.t
