(* lib/network/discovery.ml *)

(** Server discovery module.
    
    Automatically detects:
    1. Transport: TCP vs UDP
    2. Protocol: Binary vs CSV vs FIX
    
    Discovery process:
    1. Try TCP connect (with timeout)
    2. If TCP fails, try UDP probe
    3. Send probe message (Flush - minimal side effects)
    4. Analyze response to detect protocol format
*)

open Message_types

(** ============================================================================
    Discovery Types
    ============================================================================ *)

type discovery_error =
  | Connection_failed of string
  | Timeout
  | No_response
  | Socket_error of string
  | Protocol_detection_failed of string

let discovery_error_to_string = function
  | Connection_failed msg -> Printf.sprintf "Connection failed: %s" msg
  | Timeout -> "Connection timeout"
  | No_response -> "No response from server"
  | Socket_error msg -> Printf.sprintf "Socket error: %s" msg
  | Protocol_detection_failed msg -> Printf.sprintf "Protocol detection failed: %s" msg

type discovery_result = {
  transport: transport;
  protocol: protocol;
  host: string;
  port: int;
}

(** ============================================================================
    Platform Compatibility
    ============================================================================ *)

(** Set socket to non-blocking mode *)
let set_nonblocking fd =
  Unix.set_nonblock fd

(** Set socket to blocking mode *)
let set_blocking fd =
  Unix.clear_nonblock fd

(** Create TCP socket *)
let create_tcp_socket () =
  Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0

(** Create UDP socket *)
let create_udp_socket () =
  Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0

(** Resolve host to inet_addr *)
let resolve_host host =
  try
    let entry = Unix.gethostbyname host in
    if Array.length entry.Unix.h_addr_list > 0 then
      Some entry.Unix.h_addr_list.(0)
    else
      None
  with Not_found -> None

(** ============================================================================
    TCP Connection with Timeout
    ============================================================================ *)

(** Try to connect TCP with timeout (in seconds) *)
let try_tcp_connect host port timeout_sec : (Unix.file_descr, discovery_error) result =
  match resolve_host host with
  | None -> Error (Connection_failed (Printf.sprintf "Cannot resolve host: %s" host))
  | Some addr ->
    let sock = create_tcp_socket () in
    try
      set_nonblocking sock;
      let sockaddr = Unix.ADDR_INET (addr, port) in
      
      (* Non-blocking connect *)
      begin
        try Unix.connect sock sockaddr
        with Unix.Unix_error (Unix.EINPROGRESS, _, _) -> ()
           | Unix.Unix_error (Unix.EWOULDBLOCK, _, _) -> ()
      end;
      
      (* Wait for connection with select *)
      let timeout = float_of_int timeout_sec in
      let _, write_ready, _ = Unix.select [] [sock] [] timeout in
      
      if List.length write_ready = 0 then begin
        Unix.close sock;
        Error Timeout
      end else begin
        (* Check if connection succeeded *)
        try
          let err = Unix.getsockopt_error sock in
          match err with
          | None ->
            set_blocking sock;
            Ok sock
          | Some e ->
            Unix.close sock;
            Error (Connection_failed (Unix.error_message e))
        with Unix.Unix_error (e, _, _) ->
          Unix.close sock;
          Error (Connection_failed (Unix.error_message e))
      end
    with
    | Unix.Unix_error (e, _, _) ->
      (try Unix.close sock with _ -> ());
      Error (Socket_error (Unix.error_message e))

(** ============================================================================
    UDP Probe
    ============================================================================ *)

(** Send UDP probe and wait for response *)
let try_udp_probe host port timeout_sec : (Unix.file_descr, discovery_error) result =
  match resolve_host host with
  | None -> Error (Connection_failed (Printf.sprintf "Cannot resolve host: %s" host))
  | Some addr ->
    let sock = create_udp_socket () in
    try
      let sockaddr = Unix.ADDR_INET (addr, port) in
      
      (* Send probe (Flush command in CSV) *)
      let probe = Bytes.of_string "F\n" in
      let _ = Unix.sendto sock probe 0 (Bytes.length probe) [] sockaddr in
      
      (* Wait for response with timeout *)
      let timeout = float_of_int timeout_sec in
      let read_ready, _, _ = Unix.select [sock] [] [] timeout in
      
      if List.length read_ready = 0 then begin
        Unix.close sock;
        Error No_response
      end else begin
        (* Got a response - UDP is working *)
        Ok sock
      end
    with
    | Unix.Unix_error (e, _, _) ->
      (try Unix.close sock with _ -> ());
      Error (Socket_error (Unix.error_message e))

(** ============================================================================
    Protocol Detection
    ============================================================================ *)

(** Detected protocol from bytes *)
let detect_protocol_from_bytes buf len : protocol option =
  if len < 1 then None
  else
    let b0 = Bytes.get_uint8 buf 0 in
    
    (* Binary: starts with magic byte 0x4D *)
    if b0 = 0x4D then
      Some Binary
    
    (* FIX: starts with "8=FIX" - treat as separate for now *)
    else if len >= 5 && Bytes.sub_string buf 0 5 = "8=FIX" then
      None  (* FIX not yet supported, return None *)
    
    (* CSV: printable ASCII, typically starts with A, C, T, B, E *)
    else if b0 >= 0x20 && b0 < 0x7F then
      Some CSV
    
    else
      None

(** Send probe and detect protocol over existing TCP connection *)
let detect_tcp_protocol sock : (protocol, discovery_error) result =
  try
    (* Send Flush command with TCP framing (4-byte length prefix) *)
    let csv_probe = "F\n" in
    let len = String.length csv_probe in
    let frame = Bytes.make (4 + len) '\x00' in
    (* Big-endian length *)
    Bytes.set_uint8 frame 0 ((len lsr 24) land 0xFF);
    Bytes.set_uint8 frame 1 ((len lsr 16) land 0xFF);
    Bytes.set_uint8 frame 2 ((len lsr 8) land 0xFF);
    Bytes.set_uint8 frame 3 (len land 0xFF);
    Bytes.blit_string csv_probe 0 frame 4 len;
    
    let _ = Unix.write sock frame 0 (Bytes.length frame) in
    
    (* Read response with timeout *)
    let read_ready, _, _ = Unix.select [sock] [] [] 2.0 in
    if List.length read_ready = 0 then
      (* No response to Flush is normal if book is empty *)
      (* Default to CSV since we sent CSV *)
      Ok CSV
    else begin
      (* Read response *)
      let buf = Bytes.make 256 '\x00' in
      let n = Unix.read sock buf 0 256 in
      if n = 0 then
        Ok CSV  (* Connection closed, assume CSV *)
      else
        (* For TCP, first 4 bytes are length prefix - skip them *)
        let data_start = if n > 4 then 4 else 0 in
        let data_len = n - data_start in
        match detect_protocol_from_bytes (Bytes.sub buf data_start data_len) data_len with
        | Some proto -> Ok proto
        | None -> Ok CSV  (* Default to CSV *)
    end
  with
  | Unix.Unix_error (e, _, _) ->
    Error (Socket_error (Unix.error_message e))

(** Detect protocol over UDP (send probe, analyze response) *)
let detect_udp_protocol sock host port : (protocol, discovery_error) result =
  match resolve_host host with
  | None -> Error (Connection_failed "Cannot resolve host")
  | Some addr ->
    try
      let sockaddr = Unix.ADDR_INET (addr, port) in
      
      (* Send CSV Flush probe *)
      let probe = Bytes.of_string "F\n" in
      let _ = Unix.sendto sock probe 0 (Bytes.length probe) [] sockaddr in
      
      (* Wait for response *)
      let read_ready, _, _ = Unix.select [sock] [] [] 2.0 in
      if List.length read_ready = 0 then
        (* No response - could mean empty book, default to CSV *)
        Ok CSV
      else begin
        let buf = Bytes.make 256 '\x00' in
        let n, _ = Unix.recvfrom sock buf 0 256 [] in
        if n = 0 then
          Ok CSV
        else
          match detect_protocol_from_bytes buf n with
          | Some proto -> Ok proto
          | None -> Ok CSV
      end
    with
    | Unix.Unix_error (e, _, _) ->
      Error (Socket_error (Unix.error_message e))

(** ============================================================================
    Main Discovery Function
    ============================================================================ *)

(** Discover server configuration.
    
    @param host Server hostname or IP
    @param port Server port
    @param tcp_timeout Timeout for TCP connection attempt (seconds)
    @param udp_timeout Timeout for UDP probe (seconds)
    @return Discovered configuration or error
*)
let discover 
    ?(tcp_timeout=1) 
    ?(udp_timeout=2) 
    ~host 
    ~port 
    () : (discovery_result * Unix.file_descr, discovery_error) result =
  
  (* Step 1: Try TCP *)
  match try_tcp_connect host port tcp_timeout with
  | Ok tcp_sock ->
    (* TCP connected - now detect protocol *)
    begin match detect_tcp_protocol tcp_sock with
    | Ok proto ->
      Ok ({ transport = TCP; protocol = proto; host; port }, tcp_sock)
    | Error e ->
      Unix.close tcp_sock;
      Error e
    end
  
  | Error (Timeout | Connection_failed _) ->
    (* TCP failed - try UDP *)
    begin match try_udp_probe host port udp_timeout with
    | Ok udp_sock ->
      (* UDP working - detect protocol *)
      begin match detect_udp_protocol udp_sock host port with
      | Ok proto ->
        Ok ({ transport = UDP; protocol = proto; host; port }, udp_sock)
      | Error e ->
        Unix.close udp_sock;
        Error e
      end
    | Error e ->
      Error e
    end
  
  | Error e ->
    Error e

(** ============================================================================
    Quick Discovery (transport only, no protocol probe)
    ============================================================================ *)

(** Quick transport discovery - just checks if TCP or UDP is available *)
let discover_transport 
    ?(tcp_timeout=1) 
    ?(udp_timeout=2) 
    ~host 
    ~port 
    () : (transport * Unix.file_descr, discovery_error) result =
  
  (* Try TCP first *)
  match try_tcp_connect host port tcp_timeout with
  | Ok sock -> Ok (TCP, sock)
  | Error (Timeout | Connection_failed _) ->
    (* Try UDP *)
    begin match try_udp_probe host port udp_timeout with
    | Ok sock -> Ok (UDP, sock)
    | Error e -> Error e
    end
  | Error e -> Error e

(** ============================================================================
    Manual Configuration (skip discovery)
    ============================================================================ *)

(** Connect with explicit configuration (no auto-discovery) *)
let connect_explicit ~transport ~host ~port () 
    : (Unix.file_descr, discovery_error) result =
  match transport with
  | TCP ->
    begin match try_tcp_connect host port 5 with
    | Ok sock -> Ok sock
    | Error e -> Error e
    end
  | UDP ->
    match resolve_host host with
    | None -> Error (Connection_failed "Cannot resolve host")
    | Some _ ->
      let sock = create_udp_socket () in
      Ok sock

(** ============================================================================
    Discovery Result Formatting
    ============================================================================ *)

let discovery_result_to_string (r : discovery_result) : string =
  Printf.sprintf "Server at %s:%d - Transport: %s, Protocol: %s"
    r.host
    r.port
    (transport_to_string r.transport)
    (protocol_to_string r.protocol)
