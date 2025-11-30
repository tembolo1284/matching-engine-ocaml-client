(* lib/protocol/binary_codec.ml *)

(** Binary wire protocol codec for the matching engine.
    
    Wire format specification:
    - Magic byte: 0x4D ('M') as first byte
    - Message type: ASCII character ('N', 'C', 'F', 'A', 'X', 'T', 'B')
    - Integers: Network byte order (big-endian)
    - Symbols: 8 bytes, null-padded
    - Structs: Packed (no padding between fields)
    
    This is the WIRE protocol, distinct from the internal queue protocol
    defined in message_types.h.
*)

open Message_types

(** ============================================================================
    Wire Protocol Constants
    ============================================================================ *)

module Wire = struct
  (** Magic byte identifying binary protocol *)
  let magic : int = 0x4D  (* 'M' *)
  
  (** Input message type bytes *)
  let msg_new_order : int = 0x4E    (* 'N' *)
  let msg_cancel : int = 0x43       (* 'C' *)
  let msg_flush : int = 0x46        (* 'F' *)
  
  (** Output message type bytes *)
  let msg_ack : int = 0x41          (* 'A' *)
  let msg_cancel_ack : int = 0x58   (* 'X' - not 'C' to distinguish from cancel input *)
  let msg_trade : int = 0x54        (* 'T' *)
  let msg_top_of_book : int = 0x42  (* 'B' *)
  
  (** Wire message sizes (packed, no padding) *)
  let new_order_size = 27       (* Will be padded to 30 on wire *)
  let cancel_size = 10          (* Padded to 11 *)
  let flush_size = 2
  
  let ack_size = 18             (* Padded to 19 *)
  let cancel_ack_size = 18      (* Padded to 19 *)
  let trade_size = 30           (* Padded to 31 *)
  let top_of_book_size = 19     (* Padded to 20 *)
  
  (** Symbol size on wire (different from internal 16-byte!) *)
  let symbol_size = 8
  
  (** TCP framing uses 4-byte big-endian length prefix *)
  let frame_header_size = 4
end

(** ============================================================================
    Byte Order - Big Endian (Network Order)
    ============================================================================ *)

module NetworkOrder = struct
  (** Read uint8 from buffer *)
  let get_uint8 buf off =
    Bytes.get_uint8 buf off
  
  (** Read uint32 big-endian from buffer *)
  let get_uint32_be buf off =
    let b0 = Bytes.get_uint8 buf off in
    let b1 = Bytes.get_uint8 buf (off + 1) in
    let b2 = Bytes.get_uint8 buf (off + 2) in
    let b3 = Bytes.get_uint8 buf (off + 3) in
    Int32.logor
      (Int32.logor
         (Int32.shift_left (Int32.of_int b0) 24)
         (Int32.shift_left (Int32.of_int b1) 16))
      (Int32.logor
         (Int32.shift_left (Int32.of_int b2) 8)
         (Int32.of_int b3))
  
  (** Write uint8 to buffer *)
  let set_uint8 buf off v =
    Bytes.set_uint8 buf off v
  
  (** Write uint32 big-endian to buffer *)
  let set_uint32_be buf off v =
    Bytes.set_uint8 buf off (Int32.to_int (Int32.shift_right_logical v 24) land 0xFF);
    Bytes.set_uint8 buf (off + 1) (Int32.to_int (Int32.shift_right_logical v 16) land 0xFF);
    Bytes.set_uint8 buf (off + 2) (Int32.to_int (Int32.shift_right_logical v 8) land 0xFF);
    Bytes.set_uint8 buf (off + 3) (Int32.to_int v land 0xFF)
  
  (** Read string from buffer, stopping at null or max_len *)
  let get_string buf off max_len =
    let rec find_end i =
      if i >= max_len then max_len
      else if Bytes.get buf (off + i) = '\x00' then i
      else find_end (i + 1)
    in
    let len = find_end 0 in
    if len = 0 then None
    else Some (Bytes.sub_string buf off len)
  
  (** Write string to buffer, null-padding to target_len *)
  let set_string buf off s target_len =
    let len = min (String.length s) target_len in
    Bytes.blit_string s 0 buf off len;
    (* Null-pad remainder *)
    for i = len to target_len - 1 do
      Bytes.set buf (off + i) '\x00'
    done
end

(** ============================================================================
    Protocol Detection
    ============================================================================ *)

type detected_protocol =
  | Detected_CSV
  | Detected_Binary
  | Detected_FIX
  | Detected_Unknown

let detected_protocol_to_string = function
  | Detected_CSV -> "CSV"
  | Detected_Binary -> "Binary"
  | Detected_FIX -> "FIX"
  | Detected_Unknown -> "Unknown"

(** Detect protocol from response bytes.
    
    Detection rules:
    1. First byte = 0x4D ('M') → Binary protocol (magic byte)
    2. Starts with "8=FIX" → FIX protocol
    3. First byte is printable ASCII letter, second is ',' or ' ' → CSV
    4. Otherwise → Unknown
*)
let detect_protocol buf len =
  if len < 1 then Detected_Unknown
  else
    let b0 = Bytes.get_uint8 buf 0 in
    
    (* Check for Binary: magic byte 0x4D *)
    if b0 = Wire.magic then
      Detected_Binary
    
    (* Check for FIX: starts with "8=FIX" *)
    else if len >= 5 && Bytes.sub_string buf 0 5 = "8=FIX" then
      Detected_FIX
    
    (* Check for CSV: letter followed by comma/space *)
    else if len >= 2 then
      let b1 = Bytes.get_uint8 buf 1 in
      let is_letter = (b0 >= 0x41 && b0 <= 0x5A) || (b0 >= 0x61 && b0 <= 0x7A) in
      let is_separator = b1 = 0x2C (* ',' *) || b1 = 0x20 (* ' ' *) in
      if is_letter && is_separator then Detected_CSV
      else Detected_Unknown
    
    else Detected_Unknown

(** Check if buffer starts with binary magic byte *)
let is_binary_message buf len =
  len >= 1 && Bytes.get_uint8 buf 0 = Wire.magic

(** ============================================================================
    Decode Errors
    ============================================================================ *)

type decode_error =
  | Buffer_too_short of { expected: int; got: int }
  | Invalid_magic of int
  | Invalid_message_type of int
  | Invalid_side of int
  | Invalid_symbol
  | Unknown_message_type of char

let decode_error_to_string = function
  | Buffer_too_short { expected; got } ->
    Printf.sprintf "Buffer too short: expected %d bytes, got %d" expected got
  | Invalid_magic b ->
    Printf.sprintf "Invalid magic byte: 0x%02X (expected 0x4D)" b
  | Invalid_message_type t ->
    Printf.sprintf "Invalid message type byte: 0x%02X" t
  | Invalid_side s ->
    Printf.sprintf "Invalid side byte: 0x%02X (expected 'B' or 'S')" s
  | Invalid_symbol ->
    "Invalid or empty symbol"
  | Unknown_message_type c ->
    Printf.sprintf "Unknown message type: '%c' (0x%02X)" c (Char.code c)

(** ============================================================================
    Binary Encoding - Input Messages
    ============================================================================ *)

(** Encode new order to wire format (30 bytes)
    
    Layout:
      0:     magic (0x4D)
      1:     msg_type ('N' = 0x4E)
      2-5:   user_id (big-endian)
      6-13:  symbol (8 bytes, null-padded)
      14-17: price (big-endian)
      18-21: quantity (big-endian)
      22:    side ('B' or 'S')
      23-26: user_order_id (big-endian)
      27-29: padding (to reach 30 bytes)
*)
let encode_new_order (order : new_order) : bytes =
  let buf = Bytes.make 30 '\x00' in
  NetworkOrder.set_uint8 buf 0 Wire.magic;
  NetworkOrder.set_uint8 buf 1 Wire.msg_new_order;
  NetworkOrder.set_uint32_be buf 2 order.user_id;
  NetworkOrder.set_string buf 6 (Symbol.to_string order.symbol) Wire.symbol_size;
  NetworkOrder.set_uint32_be buf 14 order.price;
  NetworkOrder.set_uint32_be buf 18 order.quantity;
  NetworkOrder.set_uint8 buf 22 (side_to_uint8 order.side);
  NetworkOrder.set_uint32_be buf 23 order.user_order_id;
  (* bytes 27-29 are padding, already zero *)
  buf

(** Encode cancel order to wire format (11 bytes)
    
    Layout:
      0:     magic (0x4D)
      1:     msg_type ('C' = 0x43)
      2-5:   user_id (big-endian)
      6-9:   user_order_id (big-endian)
      10:    padding
*)
let encode_cancel (cancel : cancel_order) : bytes =
  let buf = Bytes.make 11 '\x00' in
  NetworkOrder.set_uint8 buf 0 Wire.magic;
  NetworkOrder.set_uint8 buf 1 Wire.msg_cancel;
  NetworkOrder.set_uint32_be buf 2 cancel.cancel_user_id;
  NetworkOrder.set_uint32_be buf 6 cancel.cancel_user_order_id;
  buf

(** Encode flush to wire format (2 bytes)
    
    Layout:
      0: magic (0x4D)
      1: msg_type ('F' = 0x46)
*)
let encode_flush () : bytes =
  let buf = Bytes.make 2 '\x00' in
  NetworkOrder.set_uint8 buf 0 Wire.magic;
  NetworkOrder.set_uint8 buf 1 Wire.msg_flush;
  buf

(** Encode any input message *)
let encode_input_msg (msg : input_msg) : bytes =
  match msg with
  | NewOrder order -> encode_new_order order
  | CancelOrder cancel -> encode_cancel cancel
  | Flush -> encode_flush ()

(** ============================================================================
    Binary Decoding - Output Messages
    ============================================================================ *)

(** Decode acknowledgment (19 bytes)
    
    Layout:
      0:     magic (0x4D)
      1:     msg_type ('A' = 0x41)
      2-9:   symbol (8 bytes)
      10-13: user_id (big-endian)
      14-17: user_order_id (big-endian)
      18:    padding
*)
let decode_ack buf : (ack, decode_error) result =
  if Bytes.length buf < 19 then
    Error (Buffer_too_short { expected = 19; got = Bytes.length buf })
  else
    let magic = NetworkOrder.get_uint8 buf 0 in
    if magic <> Wire.magic then
      Error (Invalid_magic magic)
    else
      match NetworkOrder.get_string buf 2 Wire.symbol_size with
      | None -> Error Invalid_symbol
      | Some sym_str ->
        match Symbol.of_string sym_str with
        | None -> Error Invalid_symbol
        | Some symbol ->
          let user_id = NetworkOrder.get_uint32_be buf 10 in
          let user_order_id = NetworkOrder.get_uint32_be buf 14 in
          Ok {
            ack_user_id = user_id;
            ack_user_order_id = user_order_id;
            ack_symbol = symbol;
          }

(** Decode cancel acknowledgment (19 bytes)
    
    Layout:
      0:     magic (0x4D)
      1:     msg_type ('X' = 0x58)
      2-9:   symbol (8 bytes)
      10-13: user_id (big-endian)
      14-17: user_order_id (big-endian)
      18:    padding
*)
let decode_cancel_ack buf : (cancel_ack, decode_error) result =
  if Bytes.length buf < 19 then
    Error (Buffer_too_short { expected = 19; got = Bytes.length buf })
  else
    let magic = NetworkOrder.get_uint8 buf 0 in
    if magic <> Wire.magic then
      Error (Invalid_magic magic)
    else
      match NetworkOrder.get_string buf 2 Wire.symbol_size with
      | None -> Error Invalid_symbol
      | Some sym_str ->
        match Symbol.of_string sym_str with
        | None -> Error Invalid_symbol
        | Some symbol ->
          let user_id = NetworkOrder.get_uint32_be buf 10 in
          let user_order_id = NetworkOrder.get_uint32_be buf 14 in
          Ok {
            cancel_ack_user_id = user_id;
            cancel_ack_user_order_id = user_order_id;
            cancel_ack_symbol = symbol;
          }

(** Decode trade (31 bytes)
    
    Layout:
      0:     magic (0x4D)
      1:     msg_type ('T' = 0x54)
      2-9:   symbol (8 bytes)
      10-13: buy_user_id (big-endian)
      14-17: buy_order_id (big-endian)
      18-21: sell_user_id (big-endian)
      22-25: sell_order_id (big-endian)
      26-29: price (big-endian)
      30-33: quantity (big-endian) -- wait, that's 34 bytes...
      
    Actually per docs it's 31 bytes, so let me re-check:
      0:     magic
      1:     msg_type
      2-9:   symbol (8)
      10-13: buy_user_id (4)
      14-17: buy_order_id (4)
      18-21: sell_user_id (4)
      22-25: sell_order_id (4)
      26-29: price (4)
      30-33: quantity (4) = 34 bytes
      
    The doc says 31 bytes... there may be a discrepancy. Using the struct
    definition which has all fields.
*)
let decode_trade buf : (trade, decode_error) result =
  (* Using actual struct size based on fields: 2 + 8 + 4*6 = 34 bytes
     But doc says 31, so there might be packed differently.
     Let's use 31 and see - quantity might be 3 bytes? Unlikely.
     Going with the field layout which gives 34 bytes minimum. *)
  let min_size = 34 in
  if Bytes.length buf < min_size then
    Error (Buffer_too_short { expected = min_size; got = Bytes.length buf })
  else
    let magic = NetworkOrder.get_uint8 buf 0 in
    if magic <> Wire.magic then
      Error (Invalid_magic magic)
    else
      match NetworkOrder.get_string buf 2 Wire.symbol_size with
      | None -> Error Invalid_symbol
      | Some sym_str ->
        match Symbol.of_string sym_str with
        | None -> Error Invalid_symbol
        | Some symbol ->
          let buy_user_id = NetworkOrder.get_uint32_be buf 10 in
          let buy_order_id = NetworkOrder.get_uint32_be buf 14 in
          let sell_user_id = NetworkOrder.get_uint32_be buf 18 in
          let sell_order_id = NetworkOrder.get_uint32_be buf 22 in
          let price = NetworkOrder.get_uint32_be buf 26 in
          let quantity = NetworkOrder.get_uint32_be buf 30 in
          Ok {
            trade_user_id_buy = buy_user_id;
            trade_user_order_id_buy = buy_order_id;
            trade_user_id_sell = sell_user_id;
            trade_user_order_id_sell = sell_order_id;
            trade_price = price;
            trade_quantity = quantity;
            (* Wire protocol doesn't include client_ids *)
            trade_buy_client_id = 0l;
            trade_sell_client_id = 0l;
            trade_symbol = symbol;
          }

(** Decode top of book (20 bytes)
    
    Layout:
      0:     magic (0x4D)
      1:     msg_type ('B' = 0x42)
      2-9:   symbol (8 bytes)
      10:    side ('B' or 'S')
      11-14: price (big-endian) - 0 if eliminated
      15-18: quantity (big-endian) - 0 if eliminated
      19:    padding
*)
let decode_top_of_book buf : (top_of_book, decode_error) result =
  if Bytes.length buf < 20 then
    Error (Buffer_too_short { expected = 20; got = Bytes.length buf })
  else
    let magic = NetworkOrder.get_uint8 buf 0 in
    if magic <> Wire.magic then
      Error (Invalid_magic magic)
    else
      match NetworkOrder.get_string buf 2 Wire.symbol_size with
      | None -> Error Invalid_symbol
      | Some sym_str ->
        match Symbol.of_string sym_str with
        | None -> Error Invalid_symbol
        | Some symbol ->
          let side_byte = NetworkOrder.get_uint8 buf 10 in
          match side_of_uint8 side_byte with
          | None -> Error (Invalid_side side_byte)
          | Some side ->
            let price = NetworkOrder.get_uint32_be buf 11 in
            let quantity = NetworkOrder.get_uint32_be buf 15 in
            Ok {
              tob_price = price;
              tob_quantity = quantity;
              tob_side = side;
              tob_symbol = symbol;
            }

(** Decode any output message based on message type byte *)
let decode_output_msg buf : (output_msg, decode_error) result =
  if Bytes.length buf < 2 then
    Error (Buffer_too_short { expected = 2; got = Bytes.length buf })
  else
    let magic = NetworkOrder.get_uint8 buf 0 in
    if magic <> Wire.magic then
      Error (Invalid_magic magic)
    else
      let msg_type = NetworkOrder.get_uint8 buf 1 in
      match msg_type with
      | t when t = Wire.msg_ack ->
        Result.map (fun a -> Ack a) (decode_ack buf)
      | t when t = Wire.msg_cancel_ack ->
        Result.map (fun c -> CancelAck c) (decode_cancel_ack buf)
      | t when t = Wire.msg_trade ->
        Result.map (fun t -> Trade t) (decode_trade buf)
      | t when t = Wire.msg_top_of_book ->
        Result.map (fun tob -> TopOfBook tob) (decode_top_of_book buf)
      | t ->
        Error (Unknown_message_type (Char.chr t))

(** ============================================================================
    TCP Framing - Big-Endian Length Prefix
    ============================================================================ *)

(** Encode message with 4-byte big-endian length prefix *)
let encode_with_frame (payload : bytes) : bytes =
  let len = Bytes.length payload in
  let buf = Bytes.make (Wire.frame_header_size + len) '\x00' in
  NetworkOrder.set_uint32_be buf 0 (Int32.of_int len);
  Bytes.blit payload 0 buf Wire.frame_header_size len;
  buf

(** Decode length prefix, returns expected payload size *)
let decode_frame_length buf : (int, decode_error) result =
  if Bytes.length buf < Wire.frame_header_size then
    Error (Buffer_too_short { expected = Wire.frame_header_size; got = Bytes.length buf })
  else
    let len = NetworkOrder.get_uint32_be buf 0 in
    Ok (Int32.to_int len)

(** ============================================================================
    Convenience Functions
    ============================================================================ *)

(** Encode input message for TCP (with length-prefix frame) *)
let encode_for_tcp (msg : input_msg) : bytes =
  let payload = encode_input_msg msg in
  encode_with_frame payload

(** Encode input message for UDP (no frame) *)
let encode_for_udp (msg : input_msg) : bytes =
  encode_input_msg msg

(** Get wire size of an input message *)
let input_msg_wire_size (msg : input_msg) : int =
  match msg with
  | NewOrder _ -> 30
  | CancelOrder _ -> 11
  | Flush -> 2

(** Minimum bytes needed to determine message type *)
let min_header_size = 2

(** Get expected size of output message based on type byte *)
let output_msg_expected_size msg_type =
  match msg_type with
  | t when t = Wire.msg_ack -> Some 19
  | t when t = Wire.msg_cancel_ack -> Some 19
  | t when t = Wire.msg_trade -> Some 34  (* Using calculated size *)
  | t when t = Wire.msg_top_of_book -> Some 20
  | _ -> None
