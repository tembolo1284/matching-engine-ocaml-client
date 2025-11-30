(* lib/network/transport.ml *)

(** Network transport layer for TCP and UDP communication.

    Provides a unified interface for sending and receiving messages
    over both TCP and UDP transports, with support for both CSV and
    Binary protocols.
*)

(** ============================================================================
    Types
    ============================================================================ *)

type connection = {
  socket: Unix.file_descr;
  transport: Message_types.transport;
  protocol: Message_types.protocol;
  host: string;
  port: int;
  remote_addr: Unix.sockaddr option;  (* For UDP *)
  mutable recv_buffer: bytes;
  mutable recv_pos: int;
}

type send_error =
  | Not_connected
  | Send_failed of string
  | Encode_error of string

type recv_error =
  | Connection_closed
  | Recv_failed of string
  | Decode_error of string
  | Timeout

let send_error_to_string = function
  | Not_connected -> "Not connected"
  | Send_failed s -> Printf.sprintf "Send failed: %s" s
  | Encode_error s -> Printf.sprintf "Encode error: %s" s

let recv_error_to_string = function
  | Connection_closed -> "Connection closed"
  | Recv_failed s -> Printf.sprintf "Receive failed: %s" s
  | Decode_error s -> Printf.sprintf "Decode error: %s" s
  | Timeout -> "Receive timeout"

(** ============================================================================
    Connection Creation
    ============================================================================ *)

let buffer_size = 65536

let create_connection 
    ~socket ~transport ~protocol ~host ~port ?remote_addr () : connection =
  {
    socket;
    transport;
    protocol;
    host;
    port;
    remote_addr;
    recv_buffer = Bytes.make buffer_size '\x00';
    recv_pos = 0;
  }

(** ============================================================================
    TCP Send/Receive
    ============================================================================ *)

(** Send all bytes, handling partial writes *)
let send_all (sock : Unix.file_descr) (data : bytes) : (int, send_error) result =
  let len = Bytes.length data in
  let sent = ref 0 in
  try
    while !sent < len do
      let n = Unix.send sock data !sent (len - !sent) [] in
      if n = 0 then raise (Failure "Connection closed");
      sent := !sent + n
    done;
    Ok !sent
  with
  | Unix.Unix_error (err, _, _) ->
    Result.Error (Send_failed (Unix.error_message err))
  | Failure msg ->
    Result.Error (Send_failed msg)

(** Send TCP message with 4-byte length prefix *)
let send_tcp_framed (conn : connection) (msg : Message_types.input_msg) 
    : (int, send_error) result =
  let payload = match conn.protocol with
    | Message_types.Binary -> Binary_codec.encode_input_msg msg
    | Message_types.CSV -> Csv_codec.encode_input_msg_bytes msg
  in
  let len = Bytes.length payload in
  let framed = Bytes.make (4 + len) '\x00' in
  (* Big-endian length prefix *)
  Bytes.set_uint8 framed 0 ((len lsr 24) land 0xFF);
  Bytes.set_uint8 framed 1 ((len lsr 16) land 0xFF);
  Bytes.set_uint8 framed 2 ((len lsr 8) land 0xFF);
  Bytes.set_uint8 framed 3 (len land 0xFF);
  Bytes.blit payload 0 framed 4 len;
  send_all conn.socket framed

(** Read exactly n bytes *)
let recv_exact (sock : Unix.file_descr) (n : int) ~timeout 
    : (bytes, recv_error) result =
  let buf = Bytes.make n '\x00' in
  let received = ref 0 in
  let start = Unix.gettimeofday () in
  try
    while !received < n do
      let elapsed = Unix.gettimeofday () -. start in
      let remaining = timeout -. elapsed in
      if remaining <= 0.0 then raise Exit;
      
      let ready, _, _ = Unix.select [sock] [] [] (min remaining 0.1) in
      if List.length ready > 0 then begin
        let r = Unix.recv sock buf !received (n - !received) [] in
        if r = 0 then raise (Failure "Connection closed");
        received := !received + r
      end
    done;
    Ok buf
  with
  | Exit -> Result.Error Timeout
  | Failure _ -> Result.Error Connection_closed
  | Unix.Unix_error (err, _, _) ->
    Result.Error (Recv_failed (Unix.error_message err))

(** Receive TCP message with 4-byte length prefix *)
let recv_tcp_framed (conn : connection) ~timeout 
    : (Message_types.output_msg, recv_error) result =
  (* Read length header *)
  match recv_exact conn.socket 4 ~timeout with
  | Result.Error e -> Result.Error e
  | Ok header ->
    let len = 
      (Bytes.get_uint8 header 0 lsl 24) lor
      (Bytes.get_uint8 header 1 lsl 16) lor
      (Bytes.get_uint8 header 2 lsl 8) lor
      (Bytes.get_uint8 header 3)
    in
    if len <= 0 || len > buffer_size then
      Result.Error (Decode_error (Printf.sprintf "Invalid frame length: %d" len))
    else
      match recv_exact conn.socket len ~timeout with
      | Result.Error e -> Result.Error e
      | Ok payload ->
        match conn.protocol with
        | Message_types.Binary ->
          Binary_codec.decode_output_msg payload 0 len
          |> Result.map_error (fun e -> 
               Decode_error (Binary_codec.decode_error_to_string e))
        | Message_types.CSV ->
          let line = Bytes.to_string payload in
          Csv_codec.parse_output_msg line
          |> Result.map_error (fun e -> 
               Decode_error (Csv_codec.parse_error_to_string e))

(** ============================================================================
    UDP Send/Receive
    ============================================================================ *)

(** Send UDP datagram *)
let send_udp (conn : connection) (msg : Message_types.input_msg) 
    : (int, send_error) result =
  match conn.remote_addr with
  | None -> Result.Error Not_connected
  | Some addr ->
    let payload = match conn.protocol with
      | Message_types.Binary -> Binary_codec.encode_input_msg msg
      | Message_types.CSV -> Csv_codec.encode_input_msg_bytes msg
    in
    try
      let n = Unix.sendto conn.socket payload 0 (Bytes.length payload) [] addr in
      Ok n
    with Unix.Unix_error (err, _, _) ->
      Result.Error (Send_failed (Unix.error_message err))

(** Receive UDP datagram *)
let recv_udp (conn : connection) ~timeout 
    : (Message_types.output_msg, recv_error) result =
  try
    let ready, _, _ = Unix.select [conn.socket] [] [] timeout in
    if List.length ready = 0 then
      Result.Error Timeout
    else begin
      let buf = Bytes.make buffer_size '\x00' in
      let (len, _src) = Unix.recvfrom conn.socket buf 0 buffer_size [] in
      if len = 0 then
        Result.Error Connection_closed
      else
        match conn.protocol with
        | Message_types.Binary ->
          Binary_codec.decode_output_msg buf 0 len
          |> Result.map_error (fun e -> 
               Decode_error (Binary_codec.decode_error_to_string e))
        | Message_types.CSV ->
          let line = Bytes.sub_string buf 0 len in
          Csv_codec.parse_output_msg line
          |> Result.map_error (fun e -> 
               Decode_error (Csv_codec.parse_error_to_string e))
    end
  with Unix.Unix_error (err, _, _) ->
    Result.Error (Recv_failed (Unix.error_message err))

(** ============================================================================
    Unified Send/Receive Interface
    ============================================================================ *)

(** Send message using appropriate transport *)
let send (conn : connection) (msg : Message_types.input_msg) 
    : (int, send_error) result =
  match conn.transport with
  | Message_types.TCP -> send_tcp_framed conn msg
  | Message_types.UDP -> send_udp conn msg

(** Receive message using appropriate transport *)
let recv ?(timeout = 5.0) (conn : connection) 
    : (Message_types.output_msg, recv_error) result =
  match conn.transport with
  | Message_types.TCP -> recv_tcp_framed conn ~timeout
  | Message_types.UDP -> recv_udp conn ~timeout

(** Try to receive (non-blocking) *)
let try_recv (conn : connection) 
    : (Message_types.output_msg option, recv_error) result =
  match conn.transport with
  | Message_types.TCP ->
    let ready, _, _ = Unix.select [conn.socket] [] [] 0.0 in
    if List.length ready = 0 then Ok None
    else recv ~timeout:0.1 conn |> Result.map (fun m -> Some m)
  | Message_types.UDP ->
    let ready, _, _ = Unix.select [conn.socket] [] [] 0.0 in
    if List.length ready = 0 then Ok None
    else recv ~timeout:0.1 conn |> Result.map (fun m -> Some m)

(** Receive multiple messages *)
let recv_batch ?(timeout = 1.0) ?(max_msgs = 100) (conn : connection)
    : (Message_types.output_msg list, recv_error) result =
  let start = Unix.gettimeofday () in
  let msgs = ref [] in
  let count = ref 0 in
  
  let rec loop () =
    let elapsed = Unix.gettimeofday () -. start in
    let remaining = timeout -. elapsed in
    
    if remaining <= 0.0 || !count >= max_msgs then
      Ok (List.rev !msgs)
    else begin
      match try_recv conn with
      | Ok None -> 
        (* No data, wait a bit *)
        let _ = Unix.select [] [] [] 0.01 in
        loop ()
      | Ok (Some msg) ->
        msgs := msg :: !msgs;
        incr count;
        loop ()
      | Result.Error Timeout ->
        Ok (List.rev !msgs)
      | Result.Error e ->
        if List.length !msgs > 0 then
          Ok (List.rev !msgs)
        else
          Result.Error e
    end
  in
  loop ()

(** Send multiple messages *)
let send_batch (conn : connection) (msgs : Message_types.input_msg list)
    : (int, send_error) result =
  let total = ref 0 in
  let rec loop = function
    | [] -> Ok !total
    | msg :: rest ->
      match send conn msg with
      | Result.Error e -> Result.Error e
      | Ok n ->
        total := !total + n;
        loop rest
  in
  loop msgs

(** ============================================================================
    Connection Info
    ============================================================================ *)

let info (conn : connection) : string =
  let transport_str = match conn.transport with
    | Message_types.TCP -> "tcp"
    | Message_types.UDP -> "udp"
  in
  let protocol_str = match conn.protocol with
    | Message_types.Binary -> "Binary"
    | Message_types.CSV -> "CSV"
  in
  Printf.sprintf "%s://%s:%d (%s)" transport_str conn.host conn.port protocol_str

let is_tcp (conn : connection) : bool = 
  conn.transport = Message_types.TCP

let is_udp (conn : connection) : bool = 
  conn.transport = Message_types.UDP

let is_binary (conn : connection) : bool = 
  conn.protocol = Message_types.Binary

let is_csv (conn : connection) : bool = 
  conn.protocol = Message_types.CSV

(** Check if socket has data available *)
let has_data (conn : connection) : bool =
  let ready, _, _ = Unix.select [conn.socket] [] [] 0.0 in
  List.length ready > 0

(** Close connection *)
let close (conn : connection) : unit =
  try Unix.close conn.socket with _ -> ()
