open Mirage

let packages =
  [ package "postgres" ~pin:"../../"
  ; package "postgres-lwt" ~pin:"../../"
  ; package "postgres-mirage" ~pin:"../../"
  ; package "logs"
  ; package "mirage-logs"
  ]
;;

let stack = generic_stackv4 default_network

let database =
  let doc = Key.Arg.info ~doc:"database to use" [ "db"; "pgdatabase" ] in
  Key.(create "pgdatabase" Arg.(opt string "postgres" doc))
;;

let port =
  let doc = Key.Arg.info ~doc:"port to use for postgresql" [ "p"; "pgport" ] in
  Key.(create "pgport" Arg.(opt int 5432 doc))
;;

let hostname =
  let doc = Key.Arg.info ~doc:"host for postgres database" [ "h"; "pghost" ] in
  Key.(create "pghost" Arg.(opt string "127.0.0.1" doc))
;;

let user =
  let doc = Key.Arg.info ~doc:"postgres user" [ "u"; "pguser" ] in
  Key.(create "pguser" Arg.(required string doc))
;;

let password =
  let doc = Key.Arg.info ~doc:"postgres password" [ "pgpassword" ] in
  Key.(create "pgpassword" Arg.(required string doc))
;;

let server =
  foreign
    "Unikernel.Make"
    ~keys:
      [ Key.abstract port
      ; Key.abstract hostname
      ; Key.abstract user
      ; Key.abstract password
      ; Key.abstract database
      ]
    ~packages
    (stackv4 @-> time @-> random @-> mclock @-> pclock @-> job)
;;

let () =
  register
    "postgres_unikernel"
    [ server
      $ stack
      $ default_time
      $ default_random
      $ default_monotonic_clock
      $ default_posix_clock
    ]
;;
