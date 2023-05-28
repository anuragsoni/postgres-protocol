(** Encrypts a password via MD5, and then encrypts it again using a random salt provided
    by the postgresql server.

    {{:https://www.postgresql.org/docs/15/protocol-flow.html#id-1.10.6.7.3}
      https://www.postgresql.org/docs/15/protocol-flow.html#id-1.10.6.7.3} *)
val hash : username:string -> password:string -> salt:string -> string
