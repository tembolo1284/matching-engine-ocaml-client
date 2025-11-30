(* lib/protocol/message_types.ml *)

(** Message types for the matching engine protocol.
    
    These types mirror the C definitions in message_types.h and 
    message_types_extended.h. All struct layouts match exactly for 
    binary protocol compatibility.
    
    Key C struct sizes:
    - new_order_msg_t: 36 bytes
    - cancel_msg_t: 24 bytes
    - ack_msg_t: 24 bytes
    - cancel_ack_msg_t: 24 bytes
    - trade_msg_t: 48 bytes
    - top_of_book_msg_t: 24 bytes
    - input_msg_t: 40 bytes (tagged union)
    - output_msg_t: 52 bytes (tagged union)
    - input_msg_envelope_t: 56 bytes
    - output_msg_envelope_t: 64 bytes (cache-aligned)
*)

(** ============================================================================
    Constants - Match message_types.h
    ============================================================================ *)

let max_symbol_length = 16

(** Symbol for top_of_book which only has 15 bytes *)
let max_tob_symbol_length = 15

(** ============================================================================
    Side - Uses ASCII 'B' (0x42) and 'S' (0x53), NOT 0/1
    ============================================================================ *)

type side = Buy | Sell

let side_to_char = function
  | Buy -> 'B'
  | Sell -> 'S'

let side_of_char = function
  | 'B' | 'b' -> Some Buy
  | 'S' | 's' -> Some Sell
  | _ -> None

(** For binary protocol - side_t is stored as the ASCII char value *)
let side_to_uint8 = function
  | Buy -> Char.code 'B'   (* 0x42 = 66 *)
  | Sell -> Char.code 'S'  (* 0x53 = 83 *)

let side_of_uint8 = function
  | 0x42 -> Some Buy  (* 'B' *)
  | 0x53 -> Some Sell (* 'S' *)
  | _ -> None

(** ============================================================================
    Order Type
    ============================================================================ *)

type order_type = Market | Limit

let order_type_to_uint8 = function
  | Market -> 0
  | Limit -> 1

let order_type_of_uint8 = function
  | 0 -> Some Market
  | 1 -> Some Limit
  | _ -> None

let order_type_of_price price =
  if price = 0l then Market else Limit

(** ============================================================================
    Message Type Tags - Match message_types.h exactly
    ============================================================================ *)

module InputMsgType = struct
  let new_order = 0
  let cancel = 1
  let flush = 2
end

module OutputMsgType = struct
  let ack = 0
  let cancel_ack = 1
  let trade = 2
  let top_of_book = 3
end

(** ============================================================================
    Symbol Module - 16-byte null-terminated string
    ============================================================================ *)

module Symbol : sig
  type t
  
  val of_string : string -> t option
  val of_string_exn : string -> t
  val to_string : t -> string
  
  (** Serialize to 16-byte null-padded buffer *)
  val to_bytes : t -> bytes
  
  (** Serialize to 15-byte null-padded buffer (for top_of_book) *)
  val to_bytes_15 : t -> bytes
  
  (** Deserialize from buffer at offset, reading up to max_len bytes *)
  val of_bytes : bytes -> int -> int -> t option
  
  val max_length : int
end = struct
  type t = string (* invariant: 0 < length <= 16 *)
  
  let max_length = max_symbol_length
  
  let of_string s =
    let len = String.length s in
    if len > 0 && len <= max_length then Some s
    else None
  
  let of_string_exn s =
    match of_string s with
    | Some t -> t
    | None -> 
      failwith (Printf.sprintf "Invalid symbol: %S (length must be 1-%d)" 
                  s max_length)
  
  let to_string t = t
  
  let to_bytes t =
    let buf = Bytes.make max_length '\x00' in
    Bytes.blit_string t 0 buf 0 (min (String.length t) max_length);
    buf
  
  let to_bytes_15 t =
    let buf = Bytes.make 15 '\x00' in
    Bytes.blit_string t 0 buf 0 (min (String.length t) 15);
    buf
  
  let of_bytes buf offset max_len =
    if Bytes.length buf < offset + max_len then None
    else begin
      let rec find_end i =
        if i >= max_len then max_len
        else if Bytes.get buf (offset + i) = '\x00' then i
        else find_end (i + 1)
      in
      let len = find_end 0 in
      if len = 0 then None
      else Some (Bytes.sub_string buf offset len)
    end
end

(** ============================================================================
    Input Message Structures
    ============================================================================ *)

(** New Order Message - matches new_order_msg_t (36 bytes)
    
    C Layout:
      0-3:   user_id (uint32_t)
      4-7:   user_order_id (uint32_t)
      8-11:  price (uint32_t) - 0 = market order
      12-15: quantity (uint32_t)
      16:    side (uint8_t) - 'B' or 'S'
      17-19: _pad[3]
      20-35: symbol[16]
*)
type new_order = {
  user_id: int32;
  user_order_id: int32;
  price: int32;
  quantity: int32;
  side: side;
  symbol: Symbol.t;
}

(** Cancel Message - matches cancel_msg_t (24 bytes)
    
    C Layout:
      0-3:   user_id (uint32_t)
      4-7:   user_order_id (uint32_t)
      8-23:  symbol[16]
*)
type cancel_order = {
  cancel_user_id: int32;
  cancel_user_order_id: int32;
  cancel_symbol: Symbol.t;
}

(** All input message types *)
type input_msg =
  | NewOrder of new_order
  | CancelOrder of cancel_order
  | Flush

(** ============================================================================
    Output Message Structures
    ============================================================================ *)

(** Acknowledgment - matches ack_msg_t (24 bytes)
    
    C Layout:
      0-3:   user_id (uint32_t)
      4-7:   user_order_id (uint32_t)
      8-23:  symbol[16]
*)
type ack = {
  ack_user_id: int32;
  ack_user_order_id: int32;
  ack_symbol: Symbol.t;
}

(** Cancel Acknowledgment - matches cancel_ack_msg_t (24 bytes)
    Same layout as ack_msg_t
*)
type cancel_ack = {
  cancel_ack_user_id: int32;
  cancel_ack_user_order_id: int32;
  cancel_ack_symbol: Symbol.t;
}

(** Trade - matches trade_msg_t (48 bytes)
    
    C Layout:
      0-3:   user_id_buy (uint32_t)
      4-7:   user_order_id_buy (uint32_t)
      8-11:  user_id_sell (uint32_t)
      12-15: user_order_id_sell (uint32_t)
      16-19: price (uint32_t)
      20-23: quantity (uint32_t)
      24-27: buy_client_id (uint32_t)
      28-31: sell_client_id (uint32_t)
      32-47: symbol[16]
*)
type trade = {
  trade_user_id_buy: int32;
  trade_user_order_id_buy: int32;
  trade_user_id_sell: int32;
  trade_user_order_id_sell: int32;
  trade_price: int32;
  trade_quantity: int32;
  trade_buy_client_id: int32;
  trade_sell_client_id: int32;
  trade_symbol: Symbol.t;
}

(** Top of Book - matches top_of_book_msg_t (24 bytes)
    
    C Layout:
      0-3:   price (uint32_t) - 0 = eliminated
      4-7:   total_quantity (uint32_t) - 0 = eliminated
      8:     side (uint8_t) - 'B' or 'S'
      9-23:  symbol[15] (only 15 bytes!)
    
    Note: price=0 AND quantity=0 means the level is eliminated
*)
type top_of_book = {
  tob_price: int32;       (* 0 = eliminated *)
  tob_quantity: int32;    (* 0 = eliminated *)
  tob_side: side;
  tob_symbol: Symbol.t;   (* max 15 chars for binary *)
}

(** Check if top-of-book indicates elimination *)
let tob_is_eliminated tob =
  tob.tob_price = 0l && tob.tob_quantity = 0l

(** Error message (not in C protocol, but useful for client) *)
type error_msg = {
  error_text: string;
}

(** All output message types *)
type output_msg =
  | Ack of ack
  | CancelAck of cancel_ack
  | Trade of trade
  | TopOfBook of top_of_book
  | Error of error_msg

(** ============================================================================
    Binary Protocol Sizes - Match C _Static_assert values
    ============================================================================ *)

module BinarySizes = struct
  (* Input message component sizes *)
  let new_order_msg = 36
  let cancel_msg = 24
  let flush_msg = 1
  let input_msg = 40  (* tagged union *)
  
  (* Output message component sizes *)
  let ack_msg = 24
  let cancel_ack_msg = 24
  let trade_msg = 48
  let top_of_book_msg = 24
  let output_msg = 52  (* tagged union *)
  
  (* Envelope sizes *)
  let input_envelope = 56
  let output_envelope = 64
  
  (* For TCP framing - 4-byte length prefix *)
  let length_prefix = 4
end

(** ============================================================================
    Transport and Protocol Discovery Types
    ============================================================================ *)

type transport = TCP | UDP

let transport_to_string = function
  | TCP -> "TCP"
  | UDP -> "UDP"

type protocol = CSV | Binary

let protocol_to_string = function
  | CSV -> "CSV"
  | Binary -> "Binary"

(** Server configuration discovered or specified *)
type server_config = {
  host: string;
  port: int;
  transport: transport;
  protocol: protocol;
  multicast: (string * int) option;  (* (group_addr, port) *)
}

(** ============================================================================
    Output Formatting - Matches C message_formatter.c
    ============================================================================ *)

(** Format price for CSV output *)
let format_price p =
  if p = 0l then "-" else Int32.to_string p

(** Format quantity for CSV output *)
let format_qty q =
  if q = 0l then "-" else Int32.to_string q

(** Convert output message to CSV string - matches C output exactly *)
let output_msg_to_csv = function
  | Ack a ->
    Printf.sprintf "A, %s, %ld, %ld"
      (Symbol.to_string a.ack_symbol)
      a.ack_user_id
      a.ack_user_order_id
  | CancelAck c ->
    Printf.sprintf "C, %s, %ld, %ld"
      (Symbol.to_string c.cancel_ack_symbol)
      c.cancel_ack_user_id
      c.cancel_ack_user_order_id
  | Trade t ->
    Printf.sprintf "T, %s, %ld, %ld, %ld, %ld, %ld, %ld"
      (Symbol.to_string t.trade_symbol)
      t.trade_user_id_buy
      t.trade_user_order_id_buy
      t.trade_user_id_sell
      t.trade_user_order_id_sell
      t.trade_price
      t.trade_quantity
  | TopOfBook tob ->
    Printf.sprintf "B, %s, %c, %s, %s"
      (Symbol.to_string tob.tob_symbol)
      (side_to_char tob.tob_side)
      (format_price tob.tob_price)
      (format_qty tob.tob_quantity)
  | Error e ->
    Printf.sprintf "E, %s" e.error_text

(** Verbose output for interactive mode *)
let output_msg_to_string_verbose = function
  | Ack a ->
    Printf.sprintf "[ACK] symbol=%s user=%ld order=%ld"
      (Symbol.to_string a.ack_symbol)
      a.ack_user_id
      a.ack_user_order_id
  | CancelAck c ->
    Printf.sprintf "[CANCEL] symbol=%s user=%ld order=%ld"
      (Symbol.to_string c.cancel_ack_symbol)
      c.cancel_ack_user_id
      c.cancel_ack_user_order_id
  | Trade t ->
    Printf.sprintf "[TRADE] %s: buy=%ld/%ld sell=%ld/%ld @ %ld x %ld"
      (Symbol.to_string t.trade_symbol)
      t.trade_user_id_buy t.trade_user_order_id_buy
      t.trade_user_id_sell t.trade_user_order_id_sell
      t.trade_price t.trade_quantity
  | TopOfBook tob ->
    let level = 
      if tob_is_eliminated tob then "EMPTY"
      else Printf.sprintf "%ld @ %ld" tob.tob_quantity tob.tob_price
    in
    Printf.sprintf "[TOB] %s %c: %s"
      (Symbol.to_string tob.tob_symbol)
      (side_to_char tob.tob_side)
      level
  | Error e ->
    Printf.sprintf "[ERROR] %s" e.error_text

(** ============================================================================
    Convenience Constructors
    ============================================================================ *)

let make_new_order ~user_id ~symbol ~price ~quantity ~side ~order_id =
  NewOrder {
    user_id;
    user_order_id = order_id;
    price;
    quantity;
    side;
    symbol;
  }

let make_cancel ~user_id ~symbol ~order_id =
  CancelOrder {
    cancel_user_id = user_id;
    cancel_user_order_id = order_id;
    cancel_symbol = symbol;
  }

let make_flush () = Flush

(** ============================================================================
    Validation
    ============================================================================ *)

let validate_new_order (o : new_order) : (unit, string) result =
  if o.quantity <= 0l then Error "quantity must be positive"
  else if o.price < 0l then Error "price cannot be negative"
  else Ok ()

let validate_input_msg = function
  | NewOrder o -> validate_new_order o
  | CancelOrder _ -> Ok ()
  | Flush -> Ok ()

(** ============================================================================
    Client ID Constants (from message_types_extended.h)
    ============================================================================ *)

(** Broadcast to all clients *)
let client_id_broadcast = 0l

let is_broadcast_client_id cid = cid = client_id_broadcast
