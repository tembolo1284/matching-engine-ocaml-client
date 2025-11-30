(* lib/network/multicast.ml *)

(** Multicast market data subscriber.

    Subscribes to UDP multicast groups for receiving market data broadcasts
    from the matching engine. Supports both CSV and Binary protocols with
    auto-detection.
*)

(** ============================================================================
    Types
    ============================================================================ *)

type subscriber_stats = {
  mutable packets_received: int;
  mutable messages_decoded: int;
  mutable decode_errors: int;
  mutable bytes_received: int;
  mutable start_time: float;
}

type subscriber = {
  group_addr: Unix.inet_addr;
  port: int;
  mutable socket: Unix.file_descr option;
  mutable protocol: Message_types.protocol option;
  stats: subscriber_stats;
}

type subscribe_error =
  | Invalid_multicast_addr of string
  | Socket_error of string
  | Join_group_failed of string

type recv_error =
  | Not_subscribed
  | Recv_failed of string
  | Decode_failed of string

let subscribe_error_to_string = function
  | Invalid_multicast_addr s -> Printf.sprintf "Invalid multicast address: %s" s
  | Socket_error s -> Printf.sprintf "Socket error: %s" s
  | Join_group_failed s -> Printf.sprintf "Failed to join multicast group: %s" s

let recv_error_to_string = function
  | Not_subscribed -> "Not subscribed to multicast group"
  | Recv_failed s -> Printf.sprintf "Receive failed: %s" s
  | Decode_failed s -> Printf.sprintf "Decode failed: %s" s

(** ============================================================================
    Multicast Address Helpers
    ============================================================================ *)

(** Check if address is in multicast range (224.0.0.0 - 239.255.255.255) *)
let is_valid_multicast_addr (addr : Unix.inet_addr) : bool =
  let s = Unix.string_of_inet_addr addr in
  match String.split_on_char '.' s with
  | [a; _; _; _] ->
    begin match int_of_string_opt a with
    | Some n -> n >= 224 && n <= 239
    | None -> false
    end
  | _ -> false

(** Parse "group:port" string *)
let parse_multicast_addr (s : string) : (Unix.inet_addr * int, string) result =
  match String.split_on_char ':' s with
  | [group; port_s] ->
    begin match int_of_string_opt port_s with
    | None -> Result.Error (Printf.sprintf "Invalid port: %s" port_s)
    | Some port ->
      if port < 1 || port > 65535 then
        Result.Error (Printf.sprintf "Port out of range: %d" port)
      else
        try
          let addr = Unix.inet_addr_of_string group in
          if is_valid_multicast_addr addr then
            Ok (addr, port)
          else
            Result.Error (Printf.sprintf "Not a multicast address: %s" group)
        with _ ->
          Result.Error (Printf.sprintf "Invalid IP address: %s" group)
    end
  | _ -> Result.Error "Expected format: group:port (e.g., 239.255.0.1:5000)"

(** ============================================================================
    Subscriber Creation and Management
    ============================================================================ *)

(** Create a new subscriber (not yet connected) *)
let create ~group_addr ~port () : subscriber = {
  group_addr;
  port;
  socket = None;
  protocol = None;
  stats = {
    packets_received = 0;
    messages_decoded = 0;
    decode_errors = 0;
    bytes_received = 0;
    start_time = 0.0;
  };
}

(** Create subscriber from "group:port" string *)
let create_from_string (addr_str : string) : (subscriber, subscribe_error) result =
  match parse_multicast_addr addr_str with
  | Result.Error e -> Result.Error (Invalid_multicast_addr e)
  | Ok (group_addr, port) -> Ok (create ~group_addr ~port ())

(** Subscribe to multicast group *)
let subscribe (sub : subscriber) : (unit, subscribe_error) result =
  try
    (* Create UDP socket *)
    let sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
    
    (* Allow address reuse *)
    Unix.setsockopt sock Unix.SO_REUSEADDR true;
    
    (* Bind to the port on any interface *)
    let bind_addr = Unix.ADDR_INET (Unix.inet_addr_any, sub.port) in
    Unix.bind sock bind_addr;
    
    (* Join multicast group *)
    (* Note: This is a simplified version. Full implementation would use
       IP_ADD_MEMBERSHIP socket option which requires platform-specific code *)
    Unix.setsockopt sock Unix.SO_BROADCAST true;
    
    sub.socket <- Some sock;
    sub.stats.start_time <- Unix.gettimeofday ();
    Ok ()
  with
  | Unix.Unix_error (err, fn, _) ->
    Result.Error (Socket_error (Printf.sprintf "%s: %s" fn (Unix.error_message err)))
  | e ->
    Result.Error (Socket_error (Printexc.to_string e))

(** Unsubscribe and close socket *)
let unsubscribe (sub : subscriber) : unit =
  match sub.socket with
  | None -> ()
  | Some sock ->
    (try Unix.close sock with _ -> ());
    sub.socket <- None

(** ============================================================================
    Receiving Messages
    ============================================================================ *)

let max_packet_size = 65535

(** Receive one message (blocking) *)
let recv (sub : subscriber) : (Message_types.output_msg, recv_error) result =
  match sub.socket with
  | None -> Result.Error Not_subscribed
  | Some sock ->
    try
      let buf = Bytes.make max_packet_size '\x00' in
      let (len, _src_addr) = Unix.recvfrom sock buf 0 max_packet_size [] in
      
      sub.stats.packets_received <- sub.stats.packets_received + 1;
      sub.stats.bytes_received <- sub.stats.bytes_received + len;
      
      (* Auto-detect protocol on first message *)
      let protocol = match sub.protocol with
        | Some p -> p
        | None ->
          let detected = Binary_codec.detect_protocol buf len in
          let p = match detected with
            | Binary_codec.Detected_Binary -> Message_types.Binary
            | Binary_codec.Detected_CSV -> Message_types.CSV
            | Binary_codec.Detected_FIX -> Message_types.CSV (* Fallback *)
            | Binary_codec.Detected_Unknown -> Message_types.CSV
          in
          sub.protocol <- Some p;
          p
      in
      
      (* Decode based on protocol *)
      let result = match protocol with
        | Message_types.Binary ->
          Binary_codec.decode_output_msg buf 0 len
          |> Result.map_error (fun e -> Decode_failed (Binary_codec.decode_error_to_string e))
        | Message_types.CSV ->
          let line = Bytes.sub_string buf 0 len in
          Csv_codec.parse_output_msg line
          |> Result.map_error (fun e -> Decode_failed (Csv_codec.parse_error_to_string e))
      in
      
      begin match result with
      | Ok _ -> sub.stats.messages_decoded <- sub.stats.messages_decoded + 1
      | Result.Error _ -> sub.stats.decode_errors <- sub.stats.decode_errors + 1
      end;
      
      result
    with
    | Unix.Unix_error (err, fn, _) ->
      Result.Error (Recv_failed (Printf.sprintf "%s: %s" fn (Unix.error_message err)))
    | e ->
      Result.Error (Recv_failed (Printexc.to_string e))

(** Try to receive (non-blocking, returns None if no data) *)
let try_recv (sub : subscriber) : (Message_types.output_msg option, recv_error) result =
  match sub.socket with
  | None -> Result.Error Not_subscribed
  | Some sock ->
    let ready, _, _ = Unix.select [sock] [] [] 0.0 in
    if List.length ready = 0 then
      Ok None
    else
      recv sub |> Result.map (fun msg -> Some msg)

(** Receive multiple messages up to timeout *)
let recv_batch ~timeout ~max_msgs (sub : subscriber) 
    : (Message_types.output_msg list, recv_error) result =
  match sub.socket with
  | None -> Result.Error Not_subscribed
  | Some sock ->
    let start = Unix.gettimeofday () in
    let msgs = ref [] in
    let count = ref 0 in
    
    let rec loop () =
      let elapsed = Unix.gettimeofday () -. start in
      let remaining = timeout -. elapsed in
      
      if remaining <= 0.0 || !count >= max_msgs then
        Ok (List.rev !msgs)
      else begin
        let ready, _, _ = Unix.select [sock] [] [] (min remaining 0.1) in
        if List.length ready > 0 then begin
          match recv sub with
          | Ok msg ->
            msgs := msg :: !msgs;
            incr count;
            loop ()
          | Result.Error e -> Result.Error e
        end else
          loop ()
      end
    in
    loop ()

(** ============================================================================
    Callback-based Receiving
    ============================================================================ *)

type message_handler = Message_types.output_msg -> unit
type error_handler = recv_error -> bool  (* Return false to stop *)

(** Run receive loop with callbacks *)
let run ~on_message ~on_error (sub : subscriber) : unit =
  let running = ref true in
  while !running do
    match recv sub with
    | Ok msg -> on_message msg
    | Result.Error e ->
      if not (on_error e) then
        running := false
  done

(** ============================================================================
    Statistics
    ============================================================================ *)

let get_stats (sub : subscriber) : subscriber_stats = sub.stats

let elapsed_time (sub : subscriber) : float =
  if sub.stats.start_time > 0.0 then
    Unix.gettimeofday () -. sub.stats.start_time
  else
    0.0

let throughput (sub : subscriber) : float =
  let elapsed = elapsed_time sub in
  if elapsed > 0.0 then
    float_of_int sub.stats.messages_decoded /. elapsed
  else
    0.0

let stats_to_string (sub : subscriber) : string =
  let elapsed = elapsed_time sub in
  let tput = throughput sub in
  Printf.sprintf
    "Packets received: %d\n\
     Messages decoded: %d\n\
     Decode errors: %d\n\
     Bytes received: %d\n\
     Elapsed: %.3fs\n\
     Throughput: %.2f msg/sec\n\
     Protocol: %s"
    sub.stats.packets_received
    sub.stats.messages_decoded
    sub.stats.decode_errors
    sub.stats.bytes_received
    elapsed
    tput
    (match sub.protocol with
     | Some Message_types.Binary -> "Binary"
     | Some Message_types.CSV -> "CSV"
     | None -> "Unknown")

(** ============================================================================
    Convenience Functions
    ============================================================================ *)

let is_subscribed (sub : subscriber) : bool =
  Option.is_some sub.socket

let get_protocol (sub : subscriber) : Message_types.protocol option =
  sub.protocol

let get_address (sub : subscriber) : string =
  Printf.sprintf "%s:%d"
    (Unix.string_of_inet_addr sub.group_addr)
    sub.port
