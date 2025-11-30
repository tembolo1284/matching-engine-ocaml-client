(* lib/network/transport.ml *)

(** Unified transport abstraction for TCP and UDP.
    
    Provides a common interface for sending and receiving messages
    regardless of underlying transport. Handles:
    - TCP framing (4-byte length prefix)
    - UDP datagrams (no framing)
    - Binary and CSV protocol encoding/decoding
*)

open Message_types

(** ============================================================================
    Transport Types
    ============================================================================ *)

type connection = {
  socket: Unix.file_descr;
  transport: transport;
  protocol: protocol;
  host: string;
  port: int;
  remote_addr: Unix.sockaddr option;  (* For UDP sendto *)
  mutable recv_buffer: bytes;         (* For TCP partial reads *)
  mutable recv_pos: int;              (* Current position in recv_buffer *)
}

type send_error =
  | Send_failed of string
  | Connection_closed
  | Encode_error of string

type recv_error =
  | Recv_failed of string
  | Recv_timeout
  | Recv_closed
  | Decode_error of string
  | Frame_error of string

let send_error_to_string = function
  | Send_failed msg -> Printf.sprintf "Send failed: %s" msg
  | Connection_closed -> "Connection closed"
  | Encode_error msg -> Printf.sprintf "Encode error: %s" msg

let recv_error_to_string = function
  | Recv_failed msg -> Printf.sprintf "Receive failed: %s" msg
  | Recv_timeout -> "Receive timeout"
  | Recv_closed -> "Connection closed by peer"
  | Decode_error msg -> Printf.sprintf "Decode error: %s" msg
  | Frame_error msg -> Printf.sprintf "Frame error: %s" msg

(** ============================================================================
    Connection Management
    ============================================================================ *)

(** Default receive buffer size *)
let default_buffer_size = 4096

(** Create connection from discovery result *)
let create ~socket ~transport ~protocol ~host ~port () : connection =
  let remote_addr = 
    match transport with
    | UDP ->
      (* For UDP, we need the remote address for sendto *)
      begin
        try
          let entry = Unix.gethostbyname host in
          if Array.length entry.Unix.h_addr_list > 0 then
            Some (Unix.ADDR_INET (entry.Unix.h_addr_list.(0), port))
          else
            None
        with Not_found -> None
      end
    | TCP -> None  (* TCP uses connected socket *)
  in
  {
    socket;
    transport;
    protocol;
    host;
    port;
    remote_addr;
    recv_buffer = Bytes.make default_buffer_size '\x00';
    recv_pos = 0;
  }

(** Close connection *)
let close conn =
  try Unix.close conn.socket
  with Unix.Unix_error _ -> ()

(** ============================================================================
    Sending Messages
    ============================================================================ *)

(** Encode message based on protocol *)
let encode_message (conn : connection) (msg : input_msg) : bytes =
  match conn.protocol with
  | Binary -> Binary_codec.encode_input_msg msg
  | CSV -> Csv_codec.encode_input_msg_bytes msg

(** Send raw bytes over TCP with framing *)
let send_tcp_framed socket (data : bytes) : (int, send_error) result =
  try
    let len = Bytes.length data in
    (* Create frame: 4-byte big-endian length + payload *)
    let frame = Bytes.make (4 + len) '\x00' in
    Bytes.set_uint8 frame 0 ((len lsr 24) land 0xFF);
    Bytes.set_uint8 frame 1 ((len lsr 16) land 0xFF);
    Bytes.set_uint8 frame 2 ((len lsr 8) land 0xFF);
    Bytes.set_uint8 frame 3 (len land 0xFF);
    Bytes.blit data 0 frame 4 len;
    
    (* Send entire frame *)
    let total = Bytes.length frame in
    let sent = ref 0 in
    while !sent < total do
      let n = Unix.write socket frame !sent (total - !sent) in
      if n = 0 then raise (Unix.Unix_error (Unix.EPIPE, "write", ""));
      sent := !sent + n
    done;
    Ok !sent
  with
  | Unix.Unix_error (Unix.EPIPE, _, _) -> Error Connection_closed
  | Unix.Unix_error (e, _, _) -> Error (Send_failed (Unix.error_message e))

(** Send raw bytes over UDP *)
let send_udp socket (data : bytes) (addr : Unix.sockaddr) : (int, send_error) result =
  try
    let n = Unix.sendto socket data 0 (Bytes.length data) [] addr in
    Ok n
  with
  | Unix.Unix_error (e, _, _) -> Error (Send_failed (Unix.error_message e))

(** Send a message over the connection *)
let send (conn : connection) (msg : input_msg) : (int, send_error) result =
  let data = encode_message conn msg in
  match conn.transport with
  | TCP -> send_tcp_framed conn.socket data
  | UDP ->
    match conn.remote_addr with
    | Some addr -> send_udp conn.socket data addr
    | None -> Error (Send_failed "No remote address for UDP")

(** ============================================================================
    Receiving Messages
    ============================================================================ *)

(** Read exactly n bytes from TCP socket *)
let read_exact socket buf offset len timeout_sec : (int, recv_error) result =
  let deadline = Unix.gettimeofday () +. timeout_sec in
  let pos = ref offset in
  let remaining = ref len in
  
  while !remaining > 0 do
    let now = Unix.gettimeofday () in
    let time_left = deadline -. now in
    if time_left <= 0.0 then
      raise (Unix.Unix_error (Unix.ETIMEDOUT, "read", ""));
    
    let ready, _, _ = Unix.select [socket] [] [] (min time_left 1.0) in
    if List.length ready = 0 then
      raise (Unix.Unix_error (Unix.ETIMEDOUT, "read", ""));
    
    let n = Unix.read socket buf !pos !remaining in
    if n = 0 then
      raise (Unix.Unix_error (Unix.ECONNRESET, "read", ""));
    pos := !pos + n;
    remaining := !remaining - n
  done;
  Ok len

(** Receive one framed message from TCP *)
let recv_tcp_framed socket timeout_sec : (bytes, recv_error) result =
  try
    (* Read 4-byte length header *)
    let header = Bytes.make 4 '\x00' in
    match read_exact socket header 0 4 timeout_sec with
    | Error e -> Error e
    | Ok _ ->
      (* Parse big-endian length *)
      let len =
        (Bytes.get_uint8 header 0 lsl 24) lor
        (Bytes.get_uint8 header 1 lsl 16) lor
        (Bytes.get_uint8 header 2 lsl 8) lor
        (Bytes.get_uint8 header 3)
      in
      
      if len <= 0 || len > 65536 then
        Error (Frame_error (Printf.sprintf "Invalid frame length: %d" len))
      else begin
        (* Read payload *)
        let payload = Bytes.make len '\x00' in
        match read_exact socket payload 0 len timeout_sec with
        | Error e -> Error e
        | Ok _ -> Ok payload
      end
  with
  | Unix.Unix_error (Unix.ETIMEDOUT, _, _) -> Error Recv_timeout
  | Unix.Unix_error (Unix.ECONNRESET, _, _) -> Error Recv_closed
  | Unix.Unix_error (e, _, _) -> Error (Recv_failed (Unix.error_message e))

(** Receive one datagram from UDP *)
let recv_udp socket timeout_sec : (bytes, recv_error) result =
  try
    let ready, _, _ = Unix.select [socket] [] [] timeout_sec in
    if List.length ready = 0 then
      Error Recv_timeout
    else begin
      let buf = Bytes.make 2048 '\x00' in
      let n, _ = Unix.recvfrom socket buf 0 (Bytes.length buf) [] in
      if n = 0 then
        Error Recv_closed
      else
        Ok (Bytes.sub buf 0 n)
    end
  with
  | Unix.Unix_error (e, _, _) -> Error (Recv_failed (Unix.error_message e))

(** Decode received bytes based on protocol *)
let decode_message (conn : connection) (data : bytes) : (output_msg, recv_error) result =
  match conn.protocol with
  | Binary ->
    begin match Binary_codec.decode_output_msg data with
    | Ok msg -> Ok msg
    | Error e -> Error (Decode_error (Binary_codec.decode_error_to_string e))
    end
  | CSV ->
    let line = Bytes.to_string data in
    begin match Csv_codec.parse_output_msg line with
    | Ok msg -> Ok msg
    | Error e -> Error (Decode_error (Csv_codec.parse_error_to_string e))
    end

(** Receive and decode one message *)
let recv ?(timeout=5.0) (conn : connection) : (output_msg, recv_error) result =
  let raw_result = 
    match conn.transport with
    | TCP -> recv_tcp_framed conn.socket timeout
    | UDP -> recv_udp conn.socket timeout
  in
  match raw_result with
  | Error e -> Error e
  | Ok data -> decode_message conn data

(** Receive raw bytes (without decoding) *)
let recv_raw ?(timeout=5.0) (conn : connection) : (bytes, recv_error) result =
  match conn.transport with
  | TCP -> recv_tcp_framed conn.socket timeout
  | UDP -> recv_udp conn.socket timeout

(** ============================================================================
    Non-blocking / Polling Operations
    ============================================================================ *)

(** Check if data is available to read (non-blocking) *)
let has_data (conn : connection) : bool =
  let ready, _, _ = Unix.select [conn.socket] [] [] 0.0 in
  List.length ready > 0

(** Try to receive without blocking - returns None if no data available *)
let try_recv (conn : connection) : (output_msg option, recv_error) result =
  if has_data conn then
    match recv ~timeout:0.1 conn with
    | Ok msg -> Ok (Some msg)
    | Error Recv_timeout -> Ok None
    | Error e -> Error e
  else
    Ok None

(** ============================================================================
    Batch Operations
    ============================================================================ *)

(** Send multiple messages *)
let send_batch (conn : connection) (msgs : input_msg list) : (int, send_error) result =
  let rec loop sent = function
    | [] -> Ok sent
    | msg :: rest ->
      match send conn msg with
      | Ok n -> loop (sent + n) rest
      | Error e -> Error e
  in
  loop 0 msgs

(** Receive multiple messages with timeout *)
let recv_batch ?(timeout=5.0) ?(max_msgs=100) (conn : connection) 
    : (output_msg list, recv_error) result =
  let deadline = Unix.gettimeofday () +. timeout in
  let rec loop acc count =
    if count >= max_msgs then Ok (List.rev acc)
    else
      let now = Unix.gettimeofday () in
      let remaining = deadline -. now in
      if remaining <= 0.0 then Ok (List.rev acc)
      else
        match recv ~timeout:(min remaining 0.5) conn with
        | Ok msg -> loop (msg :: acc) (count + 1)
        | Error Recv_timeout -> Ok (List.rev acc)
        | Error e -> 
          if List.length acc > 0 then Ok (List.rev acc)
          else Error e
  in
  loop [] 0

(** ============================================================================
    Connection Info
    ============================================================================ *)

let connection_info (conn : connection) : string =
  Printf.sprintf "%s://%s:%d (%s)"
    (match conn.transport with TCP -> "tcp" | UDP -> "udp")
    conn.host
    conn.port
    (protocol_to_string conn.protocol)

let is_tcp conn = conn.transport = TCP
let is_udp conn = conn.transport = UDP
let is_binary conn = conn.protocol = Binary
let is_csv conn = conn.protocol = CSV
