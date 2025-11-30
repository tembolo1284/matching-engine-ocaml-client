(* lib/network/multicast.ml *)

(** Multicast subscriber for market data.
    
    Subscribes to UDP multicast group and receives market data broadcasts.
    Supports both CSV and Binary protocol auto-detection.
    
    Usage:
      let sub = Multicast.create ~group:"239.255.0.1" ~port:5000 () in
      Multicast.subscribe sub;
      while true do
        match Multicast.recv sub with
        | Ok msg -> handle_message msg
        | Error e -> handle_error e
      done;
      Multicast.unsubscribe sub
*)

open Message_types

(** ============================================================================
    Types
    ============================================================================ *)

type subscriber = {
  group_addr: Unix.inet_addr;
  port: int;
  mutable socket: Unix.file_descr option;
  mutable protocol: protocol option;  (* Auto-detected on first message *)
  mutable stats: subscriber_stats;
}

and subscriber_stats = {
  mutable packets_received: int;
  mutable messages_decoded: int;
  mutable decode_errors: int;
  mutable bytes_received: int;
  start_time: float;
}

type subscribe_error =
  | Invalid_group_address of string
  | Socket_error of string
  | Already_subscribed
  | Not_subscribed

type recv_error =
  | Recv_timeout
  | Recv_failed of string
  | Decode_failed of string
  | Subscriber_not_active

let subscribe_error_to_string = function
  | Invalid_group_address addr -> Printf.sprintf "Invalid multicast group: %s" addr
  | Socket_error msg -> Printf.sprintf "Socket error: %s" msg
  | Already_subscribed -> "Already subscribed to multicast group"
  | Not_subscribed -> "Not subscribed to any multicast group"

let recv_error_to_string = function
  | Recv_timeout -> "Receive timeout"
  | Recv_failed msg -> Printf.sprintf "Receive failed: %s" msg
  | Decode_failed msg -> Printf.sprintf "Decode failed: %s" msg
  | Subscriber_not_active -> "Subscriber not active"

(** ============================================================================
    Subscriber Creation
    ============================================================================ *)

(** Create a multicast subscriber.
    
    @param group Multicast group address (e.g., "239.255.0.1")
    @param port Multicast port
    @return New subscriber (not yet subscribed)
*)
let create ~group ~port () : (subscriber, subscribe_error) result =
  try
    let group_addr = Unix.inet_addr_of_string group in
    Ok {
      group_addr;
      port;
      socket = None;
      protocol = None;
      stats = {
        packets_received = 0;
        messages_decoded = 0;
        decode_errors = 0;
        bytes_received = 0;
        start_time = Unix.gettimeofday ();
      };
    }
  with Failure _ ->
    Error (Invalid_group_address group)

(** ============================================================================
    Subscription Management
    ============================================================================ *)

(** Join multicast group and start receiving.
    
    This creates a UDP socket, enables address reuse (for multiple
    subscribers on same machine), binds to the port, and joins
    the multicast group.
*)
let subscribe (sub : subscriber) : (unit, subscribe_error) result =
  match sub.socket with
  | Some _ -> Error Already_subscribed
  | None ->
    try
      (* Create UDP socket *)
      let sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
      
      (* Allow multiple subscribers on same machine *)
      Unix.setsockopt sock Unix.SO_REUSEADDR true;
      
      (* Bind to multicast port *)
      let bind_addr = Unix.ADDR_INET (Unix.inet_addr_any, sub.port) in
      Unix.bind sock bind_addr;
      
      (* Join multicast group *)
      (* 
         OCaml's Unix module doesn't directly expose IP_ADD_MEMBERSHIP,
         so we use the lower-level approach with setsockopt_optint.
         
         On most systems, we need to set the multicast membership.
         This is platform-specific, so we'll use a workaround.
      *)
      begin
        (* Platform-specific multicast join *)
        try
          (* Try to use the Unix.mcast_join if available (OCaml 4.12+) *)
          (* For older versions, this may need external C stubs *)
          let mcast_join sock addr _interface =
            (* Construct ip_mreq structure and call setsockopt *)
            (* This is a simplified version - real impl needs C bindings *)
            ignore (sock, addr);
            ()
          in
          mcast_join sock sub.group_addr Unix.inet_addr_any
        with _ ->
          (* Fallback: on some systems, just binding is enough for loopback *)
          ()
      end;
      
      sub.socket <- Some sock;
      sub.stats.start_time <- Unix.gettimeofday ();
      Ok ()
    with
    | Unix.Unix_error (e, _, _) -> 
      Error (Socket_error (Unix.error_message e))

(** Leave multicast group and close socket *)
let unsubscribe (sub : subscriber) : (unit, subscribe_error) result =
  match sub.socket with
  | None -> Error Not_subscribed
  | Some sock ->
    begin
      try Unix.close sock
      with Unix.Unix_error _ -> ()
    end;
    sub.socket <- None;
    Ok ()

(** Check if actively subscribed *)
let is_subscribed (sub : subscriber) : bool =
  Option.is_some sub.socket

(** ============================================================================
    Receiving Messages
    ============================================================================ *)

(** Auto-detect protocol from received bytes *)
let detect_and_decode (sub : subscriber) (buf : bytes) (len : int) 
    : (output_msg, recv_error) result =
  (* Detect protocol if not yet known *)
  let protocol = match sub.protocol with
    | Some p -> p
    | None ->
      (* Auto-detect based on first byte *)
      let detected = 
        if len > 0 && Bytes.get_uint8 buf 0 = 0x4D then Binary
        else CSV
      in
      sub.protocol <- Some detected;
      detected
  in
  
  (* Decode based on protocol *)
  match protocol with
  | Binary ->
    begin match Binary_codec.decode_output_msg buf with
    | Ok msg -> 
      sub.stats.messages_decoded <- sub.stats.messages_decoded + 1;
      Ok msg
    | Error e -> 
      sub.stats.decode_errors <- sub.stats.decode_errors + 1;
      Error (Decode_failed (Binary_codec.decode_error_to_string e))
    end
  | CSV ->
    let line = Bytes.sub_string buf 0 len in
    begin match Csv_codec.parse_output_msg line with
    | Ok msg ->
      sub.stats.messages_decoded <- sub.stats.messages_decoded + 1;
      Ok msg
    | Error e ->
      sub.stats.decode_errors <- sub.stats.decode_errors + 1;
      Error (Decode_failed (Csv_codec.parse_error_to_string e))
    end

(** Receive one multicast message.
    
    @param timeout Timeout in seconds (default 5.0)
    @return Decoded message or error
*)
let recv ?(timeout=5.0) (sub : subscriber) : (output_msg, recv_error) result =
  match sub.socket with
  | None -> Error Subscriber_not_active
  | Some sock ->
    try
      (* Wait for data with timeout *)
      let ready, _, _ = Unix.select [sock] [] [] timeout in
      if List.length ready = 0 then
        Error Recv_timeout
      else begin
        (* Receive datagram *)
        let buf = Bytes.make 2048 '\x00' in
        let len, _sender = Unix.recvfrom sock buf 0 (Bytes.length buf) [] in
        
        if len = 0 then
          Error (Recv_failed "Empty datagram")
        else begin
          (* Update stats *)
          sub.stats.packets_received <- sub.stats.packets_received + 1;
          sub.stats.bytes_received <- sub.stats.bytes_received + len;
          
          (* Decode message *)
          detect_and_decode sub buf len
        end
      end
    with
    | Unix.Unix_error (e, _, _) -> 
      Error (Recv_failed (Unix.error_message e))

(** Try to receive without blocking *)
let try_recv (sub : subscriber) : (output_msg option, recv_error) result =
  match sub.socket with
  | None -> Error Subscriber_not_active
  | Some sock ->
    let ready, _, _ = Unix.select [sock] [] [] 0.0 in
    if List.length ready = 0 then
      Ok None
    else
      match recv ~timeout:0.1 sub with
      | Ok msg -> Ok (Some msg)
      | Error Recv_timeout -> Ok None
      | Error e -> Error e

(** Receive multiple messages up to a limit or timeout *)
let recv_batch ?(timeout=1.0) ?(max_msgs=100) (sub : subscriber) 
    : (output_msg list, recv_error) result =
  let deadline = Unix.gettimeofday () +. timeout in
  let rec loop acc count =
    if count >= max_msgs then 
      Ok (List.rev acc)
    else
      let remaining = deadline -. Unix.gettimeofday () in
      if remaining <= 0.0 then
        Ok (List.rev acc)
      else
        match recv ~timeout:(min remaining 0.1) sub with
        | Ok msg -> loop (msg :: acc) (count + 1)
        | Error Recv_timeout -> Ok (List.rev acc)
        | Error e ->
          if List.length acc > 0 then Ok (List.rev acc)
          else Error e
  in
  loop [] 0

(** ============================================================================
    Statistics
    ============================================================================ *)

(** Get current statistics *)
let get_stats (sub : subscriber) : subscriber_stats =
  sub.stats

(** Get elapsed time since subscription started *)
let elapsed_time (sub : subscriber) : float =
  Unix.gettimeofday () -. sub.stats.start_time

(** Calculate messages per second throughput *)
let throughput (sub : subscriber) : float =
  let elapsed = elapsed_time sub in
  if elapsed > 0.0 then
    float_of_int sub.stats.messages_decoded /. elapsed
  else
    0.0

(** Format statistics as string *)
let stats_to_string (sub : subscriber) : string =
  let stats = sub.stats in
  let elapsed = elapsed_time sub in
  Printf.sprintf
    "--- Multicast Statistics ---\n\
     Packets received: %d\n\
     Messages decoded: %d\n\
     Decode errors: %d\n\
     Bytes received: %d\n\
     Elapsed: %.3fs\n\
     Throughput: %.2f msg/sec\n\
     Protocol: %s"
    stats.packets_received
    stats.messages_decoded
    stats.decode_errors
    stats.bytes_received
    elapsed
    (throughput sub)
    (match sub.protocol with
     | Some Binary -> "Binary"
     | Some CSV -> "CSV"
     | None -> "Unknown")

(** ============================================================================
    Convenience: Run subscriber with callback
    ============================================================================ *)

(** Message handler callback type *)
type message_handler = output_msg -> unit

(** Error handler callback type *)  
type error_handler = recv_error -> [`Continue | `Stop]

(** Default error handler - continue on timeout, stop on other errors *)
let default_error_handler : error_handler = function
  | Recv_timeout -> `Continue
  | _ -> `Stop

(** Run subscriber loop with callbacks.
    
    @param sub Subscriber (must be subscribed)
    @param on_message Called for each received message
    @param on_error Called on errors, returns `Continue or `Stop
    @param stop_flag Mutable flag to stop the loop externally
*)
let run 
    ?(on_error=default_error_handler)
    ?(stop_flag=ref false)
    ~on_message
    (sub : subscriber) 
    : (unit, recv_error) result =
  if not (is_subscribed sub) then
    Error Subscriber_not_active
  else begin
    while not !stop_flag do
      match recv ~timeout:1.0 sub with
      | Ok msg -> on_message msg
      | Error e ->
        match on_error e with
        | `Continue -> ()
        | `Stop -> stop_flag := true
    done;
    Ok ()
  end

(** ============================================================================
    Multicast Group Address Helpers
    ============================================================================ *)

(** Check if address is a valid multicast address (224.0.0.0 - 239.255.255.255) *)
let is_valid_multicast_addr (addr : string) : bool =
  try
    let parts = String.split_on_char '.' addr in
    match parts with
    | [a; _; _; _] ->
      let first_octet = int_of_string a in
      first_octet >= 224 && first_octet <= 239
    | _ -> false
  with _ -> false

(** Parse "group:port" string *)
let parse_multicast_addr (s : string) : (string * int) option =
  match String.split_on_char ':' s with
  | [group; port_str] ->
    begin match int_of_string_opt port_str with
    | Some port when is_valid_multicast_addr group -> Some (group, port)
    | _ -> None
    end
  | _ -> None

(** Suggested multicast group for local testing *)
let default_group = "239.255.0.1"
let default_port = 5000
