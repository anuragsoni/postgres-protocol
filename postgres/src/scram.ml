open! Core

module Channel_binding = struct
  type t = Not_supported

  let to_gs2_header t =
    match t with
    | Not_supported -> "n,,"
  ;;
end

module Client_first_message = struct
  type t =
    { nonce : string
    ; channel_binding : Channel_binding.t
    }

  let create_nonce nonce_length =
    nonce_length
    |> Mirage_crypto_rng.generate
    |> Cstruct.to_string
    |> Base64.encode_string
  ;;

  let create () =
    let nonce = create_nonce 32 in
    { nonce; channel_binding = Channel_binding.Not_supported }
  ;;

  let message { nonce; channel_binding } =
    Channel_binding.to_gs2_header channel_binding ^ sprintf "n=,r=%s" nonce
  ;;

  let message_bare t = sprintf "n=,r=%s" t.nonce
end

module Server_first_response = struct
  type t =
    { client_first_message : Client_first_message.t
    ; payload : string
    ; salt : string
    ; iterations : int
    ; raw_message : string
    }

  let take_till ch buf =
    match Parser_utils.take_till (fun ch' -> Char.equal ch ch') buf with
    | "" -> None
    | v -> Some v
  ;;

  let rec parse iobuf =
    if Iobuf.is_empty iobuf
    then Some []
    else (
      let%bind.Option key = take_till '=' iobuf in
      let%bind.Option value = take_till ',' iobuf in
      let%map.Option rest = parse iobuf in
      (key, value) :: rest)
  ;;

  let parse client_first_message iobuf =
    let raw_message = Iobuf.to_string iobuf in
    match parse iobuf with
    | None -> None
    | Some xs ->
      Option.try_with_join (fun () ->
        let find key = List.Assoc.find xs key ~equal:String.Caseless.equal in
        let%map.Option payload = find "r"
        and salt =
          let%map.Option salt = find "s" in
          Base64.decode_exn salt
        and iterations =
          let%map.Option i = find "i" in
          Int.of_string i
        in
        { payload; salt; iterations; raw_message; client_first_message })
  ;;

  let client_proof_and_server_signature ~password (t : t) =
    let salted_password =
      Pbkdf.pbkdf2
        ~prf:`SHA256
        ~password:(Cstruct.of_string password)
        ~salt:(Cstruct.of_string t.salt)
        ~count:t.iterations
        ~dk_len:32l
    in
    let client_key =
      Mirage_crypto.Hash.SHA256.hmac (Cstruct.of_string "Client Key") ~key:salted_password
    in
    let stored_key = Mirage_crypto.Hash.SHA256.digest client_key in
    let client_message_without_proof = sprintf "c=biws,r=%s" t.payload in
    let auth_message =
      String.concat
        ~sep:","
        [ Client_first_message.message_bare t.client_first_message
        ; t.raw_message
        ; client_message_without_proof
        ]
    in
    let client_signature =
      Mirage_crypto.Hash.SHA256.hmac ~key:stored_key (Cstruct.of_string auth_message)
    in
    let client_proof =
      Mirage_crypto.Uncommon.Cs.xor client_key client_signature
      |> Cstruct.to_string
      |> Base64.encode_exn
    in
    let client_proof = sprintf "p=%s" client_proof in
    let server_key =
      Mirage_crypto.Hash.SHA256.hmac ~key:salted_password (Cstruct.of_string "Server Key")
    in
    let server_signature =
      Mirage_crypto.Hash.SHA256.hmac ~key:server_key (Cstruct.of_string auth_message)
    in
    let client_final =
      String.concat ~sep:"," [ client_message_without_proof; client_proof ]
    in
    client_final, Base64.encode_exn (Cstruct.to_string server_signature)
  ;;
end
