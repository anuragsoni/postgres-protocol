open Core

let hash ~username ~password ~salt =
  let digest = Md5.digest_string (password ^ username) in
  let hex = Md5.to_hex digest in
  let digest = Md5.digest_string (hex ^ salt) in
  let hex = Md5.to_hex digest in
  sprintf "md5%s" hex
;;
