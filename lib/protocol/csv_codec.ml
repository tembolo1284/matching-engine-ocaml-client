(* lib/protocol/csv_codec.ml *)

(** CSV protocol codec for the matching engine.
    
    CSV format specification:
    - Fields separated by ", " (comma-space)
    - Lines terminated by newline
    - No quoting or escaping needed (no commas in field values)
    
    Input formats:
      N, userId, symbol, price, qty, side, userOrderId
      C, userId, userOrderId
      F
    
    Output formats:
      A, symbol, userId, userOrderId
      C, symbol, userId, userOrderId
      T, symbol, buyUserId, buyOrderId, sellUserId, sellOrderId, price, qty
      B, symbol, side, price, qty    (price/qty can be "-" for eliminated)
*)

open Message_types

(** ============================================================================
    Parse Errors
    ============================================================================ *)

type parse_error =
  | Empty_input
  | Unknown_message_type of char
  | Missing_fields of { expected: int; got: int }
  | Invalid_integer of string
  | Invalid_side of string
  | Invalid_symbol of string
  | Malformed_line of string

let parse_error_to_string = function
  | Empty_input -> "Empty input"
  | Unknown_message_type c -> Printf.sprintf "Unknown message type: '%c'" c
  | Missing_fields { expected; got } ->
    Printf.sprintf "Missing fields: expected %d, got %d" expected got
  | Invalid_integer s -> Printf.sprintf "Invalid integer: %S" s
  | Invalid_side s -> Printf.sprintf "Invalid side: %S (expected 'B' or 'S')" s
  | Invalid_symbol s -> Printf.sprintf "Invalid symbol: %S" s
  | Malformed_line s -> Printf.sprintf "Malformed line: %S" s

(** ============================================================================
    Parsing Helpers
    ============================================================================ *)

(** Split string by ", " delimiter *)
let split_fields (s : string) : string list =
  let s = String.trim s in
  String.split_on_char ',' s
  |> List.map String.trim
  |> List.filter (fun s -> String.length s > 0)

(** Parse int32 from string *)
let parse_int32 (s : string) : (int32, parse_error) result =
  match Int32.of_string_opt (String.trim s) with
  | Some n -> Ok n
  | None -> Error (Invalid_integer s)

(** Parse side from string *)
let parse_side (s : string) : (side, parse_error) result =
  match String.trim s with
  | "B" | "b" -> Ok Buy
  | "S" | "s" -> Ok Sell
  | other -> Error (Invalid_side other)

(** Parse symbol from string *)
let parse_symbol (s : string) : (Symbol.t, parse_error) result =
  let s = String.trim s in
  match Symbol.of_string s with
  | Some sym -> Ok sym
  | None -> Error (Invalid_symbol s)

(** Parse optional price (can be "-" for eliminated) *)
let parse_price_opt (s : string) : (int32, parse_error) result =
  let s = String.trim s in
  if s = "-" then Ok 0l
  else parse_int32 s

(** Parse optional quantity (can be "-" for eliminated) *)
let parse_qty_opt (s : string) : (int32, parse_error) result =
  let s = String.trim s in
  if s = "-" then Ok 0l
  else parse_int32 s

(** ============================================================================
    Input Message Parsing
    ============================================================================ *)

(** Parse new order: N, userId, symbol, price, qty, side, userOrderId *)
let parse_new_order (fields : string list) : (input_msg, parse_error) result =
  match fields with
  | [_; user_id; symbol; price; qty; side; order_id] ->
    let open Result in
    parse_int32 user_id >>= fun user_id ->
    parse_symbol symbol >>= fun symbol ->
    parse_int32 price >>= fun price ->
    parse_int32 qty >>= fun quantity ->
    parse_side side >>= fun side ->
    parse_int32 order_id >>= fun order_id ->
    Ok (NewOrder {
      user_id;
      user_order_id = order_id;
      price;
      quantity;
      side;
      symbol;
    })
  | _ ->
    Error (Missing_fields { expected = 7; got = List.length fields })

(** Parse cancel order: C, userId, userOrderId 
    Note: Wire format doesn't include symbol for cancel *)
let parse_cancel (fields : string list) : (input_msg, parse_error) result =
  match fields with
  | [_; user_id; order_id] ->
    let open Result in
    parse_int32 user_id >>= fun user_id ->
    parse_int32 order_id >>= fun order_id ->
    (* Cancel on wire doesn't have symbol - use empty placeholder *)
    Ok (CancelOrder {
      cancel_user_id = user_id;
      cancel_user_order_id = order_id;
      cancel_symbol = Symbol.of_string_exn "?";  (* Placeholder *)
    })
  | _ ->
    Error (Missing_fields { expected = 3; got = List.length fields })

(** Parse flush: F *)
let parse_flush (fields : string list) : (input_msg, parse_error) result =
  match fields with
  | [_] -> Ok Flush
  | _ -> Error (Missing_fields { expected = 1; got = List.length fields })

(** Parse any input message from CSV line *)
let parse_input_msg (line : string) : (input_msg, parse_error) result =
  let line = String.trim line in
  if String.length line = 0 then
    Error Empty_input
  else
    let fields = split_fields line in
    match fields with
    | [] -> Error Empty_input
    | msg_type :: _ ->
      match msg_type with
      | "N" | "n" -> parse_new_order fields
      | "C" | "c" -> parse_cancel fields
      | "F" | "f" -> parse_flush fields
      | s when String.length s > 0 -> 
        Error (Unknown_message_type s.[0])
      | _ -> Error (Malformed_line line)

(** ============================================================================
    Output Message Parsing
    ============================================================================ *)

(** Parse ack: A, symbol, userId, userOrderId *)
let parse_ack (fields : string list) : (output_msg, parse_error) result =
  match fields with
  | [_; symbol; user_id; order_id] ->
    let open Result in
    parse_symbol symbol >>= fun symbol ->
    parse_int32 user_id >>= fun user_id ->
    parse_int32 order_id >>= fun order_id ->
    Ok (Ack {
      ack_symbol = symbol;
      ack_user_id = user_id;
      ack_user_order_id = order_id;
    })
  | _ ->
    Error (Missing_fields { expected = 4; got = List.length fields })

(** Parse cancel ack: C, symbol, userId, userOrderId *)
let parse_cancel_ack (fields : string list) : (output_msg, parse_error) result =
  match fields with
  | [_; symbol; user_id; order_id] ->
    let open Result in
    parse_symbol symbol >>= fun symbol ->
    parse_int32 user_id >>= fun user_id ->
    parse_int32 order_id >>= fun order_id ->
    Ok (CancelAck {
      cancel_ack_symbol = symbol;
      cancel_ack_user_id = user_id;
      cancel_ack_user_order_id = order_id;
    })
  | _ ->
    Error (Missing_fields { expected = 4; got = List.length fields })

(** Parse trade: T, symbol, buyUserId, buyOrderId, sellUserId, sellOrderId, price, qty *)
let parse_trade (fields : string list) : (output_msg, parse_error) result =
  match fields with
  | [_; symbol; buy_uid; buy_oid; sell_uid; sell_oid; price; qty] ->
    let open Result in
    parse_symbol symbol >>= fun symbol ->
    parse_int32 buy_uid >>= fun buy_user_id ->
    parse_int32 buy_oid >>= fun buy_order_id ->
    parse_int32 sell_uid >>= fun sell_user_id ->
    parse_int32 sell_oid >>= fun sell_order_id ->
    parse_int32 price >>= fun price ->
    parse_int32 qty >>= fun quantity ->
    Ok (Trade {
      trade_symbol = symbol;
      trade_user_id_buy = buy_user_id;
      trade_user_order_id_buy = buy_order_id;
      trade_user_id_sell = sell_user_id;
      trade_user_order_id_sell = sell_order_id;
      trade_price = price;
      trade_quantity = quantity;
      trade_buy_client_id = 0l;
      trade_sell_client_id = 0l;
    })
  | _ ->
    Error (Missing_fields { expected = 8; got = List.length fields })

(** Parse top of book: B, symbol, side, price, qty *)
let parse_top_of_book (fields : string list) : (output_msg, parse_error) result =
  match fields with
  | [_; symbol; side; price; qty] ->
    let open Result in
    parse_symbol symbol >>= fun symbol ->
    parse_side side >>= fun side ->
    parse_price_opt price >>= fun price ->
    parse_qty_opt qty >>= fun quantity ->
    Ok (TopOfBook {
      tob_symbol = symbol;
      tob_side = side;
      tob_price = price;
      tob_quantity = quantity;
    })
  | _ ->
    Error (Missing_fields { expected = 5; got = List.length fields })

(** Parse any output message from CSV line *)
let parse_output_msg (line : string) : (output_msg, parse_error) result =
  let line = String.trim line in
  if String.length line = 0 then
    Error Empty_input
  else
    let fields = split_fields line in
    match fields with
    | [] -> Error Empty_input
    | msg_type :: _ ->
      match msg_type with
      | "A" | "a" -> parse_ack fields
      | "C" | "c" -> parse_cancel_ack fields  (* Output 'C' is cancel ack *)
      | "T" | "t" -> parse_trade fields
      | "B" | "b" -> parse_top_of_book fields
      | s when String.length s > 0 ->
        Error (Unknown_message_type s.[0])
      | _ -> Error (Malformed_line line)

(** ============================================================================
    Output Message Encoding (for display, matches C server output)
    ============================================================================ *)

(** Encode input message to CSV string *)
let encode_input_msg (msg : input_msg) : string =
  match msg with
  | NewOrder o ->
    Printf.sprintf "N, %ld, %s, %ld, %ld, %c, %ld"
      o.user_id
      (Symbol.to_string o.symbol)
      o.price
      o.quantity
      (side_to_char o.side)
      o.user_order_id
  | CancelOrder c ->
    Printf.sprintf "C, %ld, %ld"
      c.cancel_user_id
      c.cancel_user_order_id
  | Flush ->
    "F"

(** Encode input message with newline for sending *)
let encode_input_msg_line (msg : input_msg) : string =
  encode_input_msg msg ^ "\n"

(** Encode input message to bytes for sending *)
let encode_input_msg_bytes (msg : input_msg) : bytes =
  Bytes.of_string (encode_input_msg_line msg)

(** Output message encoding is already in Message_types.output_msg_to_csv *)

(** ============================================================================
    TCP Framing for CSV
    ============================================================================ *)

(** Encode CSV message with 4-byte big-endian length prefix for TCP *)
let encode_for_tcp (msg : input_msg) : bytes =
  let csv = encode_input_msg_line msg in
  let payload = Bytes.of_string csv in
  let len = Bytes.length payload in
  let buf = Bytes.make (4 + len) '\x00' in
  (* Big-endian length prefix *)
  Bytes.set_uint8 buf 0 ((len lsr 24) land 0xFF);
  Bytes.set_uint8 buf 1 ((len lsr 16) land 0xFF);
  Bytes.set_uint8 buf 2 ((len lsr 8) land 0xFF);
  Bytes.set_uint8 buf 3 (len land 0xFF);
  Bytes.blit payload 0 buf 4 len;
  buf

(** Encode CSV message for UDP (no framing, just the line) *)
let encode_for_udp (msg : input_msg) : bytes =
  encode_input_msg_bytes msg

(** ============================================================================
    Helpers for Result Monad
    ============================================================================ *)

module Result = struct
  let (>>=) r f = match r with
    | Ok v -> f v
    | Error e -> Error e
  
  let map f r = match r with
    | Ok v -> Ok (f v)
    | Error e -> Error e
end
