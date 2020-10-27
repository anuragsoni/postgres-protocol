let src = Logs.Src.create "postgres.core"

include (val Logs.src_log src : Logs.LOG)
