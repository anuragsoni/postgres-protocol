open Core

type error =
  | Not_enough_data_for_message_frame of { need_at_least : int }
  | Fail of string
  | Exn of (Exn.t[@sexp.opaque])
[@@deriving sexp_of]

let parse_cstring buf = Parser_utils.take_till (fun ch -> Char.equal ch '\x00') buf

module Authentication = struct
  type t =
    | AuthOk
    | KerberosV5
    | ClearTextPassword
    | Md5Password of string
    | SCMCredential
    | GSS
    | GSSContinue of string
    | SSPI
    | SASL of string
    | SASLContinue of string
    | SASLFinal of string
  [@@deriving sexp_of]

  let parse iobuf =
    match Iobuf.Consume.int32_be iobuf with
    | 0 -> Ok AuthOk
    | 2 -> Ok KerberosV5
    | 3 -> Ok ClearTextPassword
    | 5 ->
      let salt = Iobuf.Consume.stringo iobuf ~len:4 in
      Ok (Md5Password salt)
    | 6 -> Ok SCMCredential
    | 7 -> Ok GSS
    | 8 ->
      let auth_data = Iobuf.Consume.stringo iobuf in
      Ok (GSSContinue auth_data)
    | 9 -> Ok SSPI
    | 10 ->
      let body = Iobuf.Consume.stringo iobuf in
      Ok (SASL body)
    | 11 ->
      let body = Iobuf.Consume.stringo iobuf in
      Ok (SASLContinue body)
    | 12 ->
      let key = Parser_utils.take_till (fun ch -> Char.equal ch '=') iobuf in
      (match key with
       | "v" ->
         let body = Iobuf.Consume.stringo iobuf in
         Ok (SASLFinal body)
       | _ -> Error (Fail (sprintf "Invalid payload for SASL final message")))
    | n -> Error (Fail (sprintf "Unknown auth kind: %d" n))
  ;;
end

module Ready_for_query = struct
  type t =
    | Idle
    | Transaction_block
    | Failed_transaction
  [@@deriving sexp_of]

  let parse iobuf =
    match Iobuf.Peek.char iobuf ~pos:0 with
    | 'I' ->
      Iobuf.advance iobuf 1;
      Ok Idle
    | 'T' ->
      Iobuf.advance iobuf 1;
      Ok Transaction_block
    | 'E' ->
      Iobuf.advance iobuf 1;
      Ok Failed_transaction
    | c -> Error (Fail (sprintf "Unknown response for ready query: %C" c))
  ;;
end

module Backend_key_data = struct
  type t =
    { pid : int
    ; secret : int
    }
  [@@deriving sexp_of]

  let parse iobuf =
    let pid = Iobuf.Consume.int32_be iobuf in
    let secret = Iobuf.Consume.int32_be iobuf in
    Ok { pid; secret }
  ;;
end

module Make_error_notice () = struct
  type kind =
    | Severity
    | Non_localized_severity
    | Code
    | Message
    | Detail
    | Hint
    | Position
    | Internal_position
    | Internal_query
    | Where
    | Schema_name
    | Table_name
    | Column_name
    | Datatype_name
    | Constraint_name
    | File
    | Line
    | Routine
    | Unknown of char
  [@@deriving sexp_of]

  let kind_of_char ch =
    match ch with
    | 'S' -> Severity
    | 'V' -> Non_localized_severity
    | 'C' -> Code
    | 'M' -> Message
    | 'D' -> Detail
    | 'H' -> Hint
    | 'P' -> Position
    | 'p' -> Internal_position
    | 'q' -> Internal_query
    | 'W' -> Where
    | 's' -> Schema_name
    | 't' -> Table_name
    | 'c' -> Column_name
    | 'd' -> Datatype_name
    | 'n' -> Constraint_name
    | 'F' -> File
    | 'L' -> Line
    | 'R' -> Routine
    | c -> Unknown c
  ;;

  type error_or_notice =
    { kind : kind
    ; message : string
    }
  [@@deriving sexp_of]

  type t = error_or_notice list [@@deriving sexp_of]

  let parse iobuf =
    let rec loop acc =
      match Iobuf.Consume.char iobuf with
      | '\x00' -> Ok (List.rev acc)
      | ch ->
        let kind = kind_of_char ch in
        let message = parse_cstring iobuf in
        loop ({ kind; message } :: acc)
    in
    loop []
  ;;
end

module Error_response = Make_error_notice ()
module Notice_response = Make_error_notice ()

module Parameter_status = struct
  type t =
    { name : string
    ; value : string
    }
  [@@deriving sexp_of]

  let parse iobuf =
    let name = parse_cstring iobuf in
    let value = parse_cstring iobuf in
    Ok { name; value }
  ;;
end

module Data_row = struct
  type t = string option array [@@deriving sexp_of]

  let parse iobuf =
    let len = Iobuf.Consume.int16_be iobuf in
    if len = 0
    then Ok [||]
    else (
      let parse_row_element iobuf =
        match Iobuf.Consume.int32_be iobuf with
        | -1 -> None
        | len -> Some (Iobuf.Consume.stringo iobuf ~len)
      in
      let head = parse_row_element iobuf in
      let arr = Array.init len ~f:(fun _ -> head) in
      for i = 1 to len - 1 do
        Array.unsafe_set arr i (parse_row_element iobuf)
      done;
      Ok arr)
  ;;
end

type message =
  | Authentication of Authentication.t
  | BackendKeyData of Backend_key_data.t
  | ReadyForQuery of Ready_for_query.t
  | BindComplete
  | ErrorResponse of Error_response.t
  | NoticeResponse of Notice_response.t
  | CloseComplete
  | ParseComplete
  | ParameterStatus of Parameter_status.t
  | DataRow of Data_row.t
  | CommandComplete of string
[@@deriving sexp_of]

(** Parse a postgres backend message:

    - Check if the iobuf has enough data to potentially parse the frame header. Frame
      header requires 5 bytes.
    - If frame header was read successfully, check if the iobuf has enough content to
      parse the full frame length specified in the header.
    - If iobuf has enough data, resize the iobuf's window so any frame parser can only
      access [n - 5] bytes where [n] is the frame length specified in the frame header.
    - If a frame parser is successful, verify that the parser actually consumed [n] bytes,
      where [n] is the frame length specified in the frame header.
    - Update the iobuf's window to position it over the remaining unconsumed bytes in the
      buffer. This will leave the buffer in a state where the starting point of the window
      is the beginning of the next message frame's header. *)
let parse iobuf =
  let open Result.Let_syntax in
  if Iobuf.length iobuf < 5
  then Error (Not_enough_data_for_message_frame { need_at_least = 5 })
  else (
    let frame_size = Iobuf.Peek.int32_be iobuf ~pos:1 + 1 in
    if Iobuf.length iobuf < frame_size
    then Error (Not_enough_data_for_message_frame { need_at_least = frame_size })
    else (
      let frame_kind = Iobuf.Peek.char iobuf ~pos:0 in
      let hi_bound = Iobuf.Hi_bound.window iobuf in
      Iobuf.resize iobuf ~len:frame_size;
      Iobuf.advance iobuf 5;
      let%bind message =
        try
          match frame_kind with
          | 'R' ->
            let%map.Result message = Authentication.parse iobuf in
            Authentication message
          | 'E' ->
            let%map.Result message = Error_response.parse iobuf in
            ErrorResponse message
          | 'N' ->
            let%map.Result message = Notice_response.parse iobuf in
            NoticeResponse message
          | 'K' ->
            let%map.Result message = Backend_key_data.parse iobuf in
            BackendKeyData message
          | 'Z' ->
            let%map.Result message = Ready_for_query.parse iobuf in
            ReadyForQuery message
          | 'S' ->
            let%map.Result message = Parameter_status.parse iobuf in
            ParameterStatus message
          | 'D' ->
            let%map.Result message = Data_row.parse iobuf in
            DataRow message
          | 'C' -> Ok (CommandComplete (Iobuf.Consume.stringo iobuf))
          | '1' -> Ok ParseComplete
          | '2' -> Ok BindComplete
          | '3' -> Ok CloseComplete
          | ch -> Error (Fail (sprintf "Unknown message kind: %c" ch))
        with
        | exn -> Error (Exn exn)
      in
      (* Verify that our message parsers actually consumed the entire message payload
         length specified in the frame header. *)
      match Iobuf.is_empty iobuf with
      | false -> Error (Fail "Backend message parser did not consume entire frame")
      | true ->
        Iobuf.bounded_flip_hi iobuf hi_bound;
        Ok message))
;;
