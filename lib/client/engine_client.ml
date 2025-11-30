(* lib/client/engine_client.ml *)

(** High-level client API for the matching engine.

    Provides a simple interface for:
    - Connecting to the server (with auto-discovery)
    - Placing orders (buy/sell)
    - Canceling orders
    - Receiving responses
*)

(** ============================================================================
    Types
    ============================================================================ *)

type client_config = {
  host: string;
  port: int;
  transport: Message_types.transport option;  (* None = auto-discover *)
  protocol: Message_types.protocol option;    (* None = auto-discover *)
  timeout: float;
  verbose: bool;
}

type client = {
  config: client_config;
  mutable connection: Transport.connection option;
  mutable user_id: int32;
  mutable next_order_id: int32;
  mutable connected: bool;
}

type client_error =
  | Connection_failed of string
  | Discovery_failed of string
  | Not_connected
  | Send_error of Transport.send_error
  | Recv_error of Transport.recv_error
  | Invalid_symbol of string

let client_error_to_string = function
  | Connection_failed s -> Printf.sprintf "Connection failed: %s" s
  | Discovery_failed s -> Printf.sprintf "Discovery failed: %s" s
  | Not_connected -> "Not connected"
  | Send_error e -> Transport.send_error_to_string e
  | Recv_error e -> Transport.recv_error_to_string e
  | Invalid_symbol s -> Printf.sprintf "Invalid symbol: %s" s

(** ============================================================================
    Configuration
    ============================================================================ *)

let default_config ~host ~port () : client_config = {
  host;
  port;
  transport = None;
  protocol = None;
  timeout = 5.0;
  verbose = false;
}

(** ============================================================================
    Client Creation and Connection
    ============================================================================ *)

let create (config : client_config) : client = {
  config;
  connection = None;
  user_id = 1l;
  next_order_id = 1l;
  connected = false;
}

(** Connect to server, with auto-discovery if transport/protocol not specified *)
let connect (client : client) : (unit, client_error) result =
  let cfg = client.config in
  
  (* Determine transport and protocol *)
  let get_transport_protocol () =
    match cfg.transport, cfg.protocol with
    | Some t, Some p -> Ok (t, p)
    | _ ->
      (* Need to discover *)
      match Discovery.discover ~host:cfg.host ~port:cfg.port ~timeout:cfg.timeout () with
      | Result.Error e -> Result.Error (Discovery_failed (Discovery.discovery_error_to_string e))
      | Ok result ->
        let transport = match cfg.transport with
          | Some t -> t
          | None -> result.Discovery.transport
        in
        let protocol = match cfg.protocol with
          | Some p -> p
          | None -> result.Discovery.protocol
        in
        Ok (transport, protocol)
  in
  
  match get_transport_protocol () with
  | Result.Error e -> Result.Error e
  | Ok (transport, protocol) ->
    (* Create socket and connect *)
    try
      let sock_type = match transport with
        | Message_types.TCP -> Unix.SOCK_STREAM
        | Message_types.UDP -> Unix.SOCK_DGRAM
      in
      let sock = Unix.socket Unix.PF_INET sock_type 0 in
      
      let host_entry = Unix.gethostbyname cfg.host in
      let addr = Unix.ADDR_INET (host_entry.Unix.h_addr_list.(0), cfg.port) in
      
      begin match transport with
      | Message_types.TCP ->
        Unix.connect sock addr;
        let conn = Transport.create_connection
          ~socket:sock ~transport ~protocol
          ~host:cfg.host ~port:cfg.port ()
        in
        client.connection <- Some conn
      | Message_types.UDP ->
        (* UDP doesn't need connect, but we store the remote address *)
        let conn = Transport.create_connection
          ~socket:sock ~transport ~protocol
          ~host:cfg.host ~port:cfg.port ~remote_addr:addr ()
        in
        client.connection <- Some conn
      end;
      
      client.connected <- true;
      
      if cfg.verbose then
        Printf.printf "Connected: %s\n%!" (info client);
      
      Ok ()
    with
    | Unix.Unix_error (err, fn, _) ->
      Result.Error (Connection_failed (Printf.sprintf "%s: %s" fn (Unix.error_message err)))
    | Not_found ->
      Result.Error (Connection_failed (Printf.sprintf "Unknown host: %s" cfg.host))
    | e ->
      Result.Error (Connection_failed (Printexc.to_string e))

(** Disconnect from server *)
let disconnect (client : client) : unit =
  match client.connection with
  | None -> ()
  | Some conn ->
    Transport.close conn;
    client.connection <- None;
    client.connected <- false

(** ============================================================================
    Sending Messages
    ============================================================================ *)

(** Get next order ID and increment counter *)
let next_order_id (client : client) : int32 =
  let id = client.next_order_id in
  client.next_order_id <- Int32.add client.next_order_id 1l;
  id

(** Send a new order *)
let send_order (client : client) 
    ~symbol ~price ~quantity ~side ?order_id () 
    : (int32, client_error) result =
  match client.connection with
  | None -> Result.Error Not_connected
  | Some conn ->
    let oid = match order_id with
      | Some id -> id
      | None -> next_order_id client
    in
    let order : Message_types.new_order = {
      user_id = client.user_id;
      user_order_id = oid;
      price;
      quantity;
      side;
      symbol;
    } in
    match Transport.send conn (Message_types.NewOrder order) with
    | Result.Error e -> Result.Error (Send_error e)
    | Ok _ -> Ok oid

(** Send cancel order *)
let send_cancel (client : client) ~order_id () : (unit, client_error) result =
  match client.connection with
  | None -> Result.Error Not_connected
  | Some conn ->
    let cancel : Message_types.cancel_order = {
      cancel_user_id = client.user_id;
      cancel_user_order_id = order_id;
      cancel_symbol = Message_types.Symbol.of_string_exn "?";  (* Not used in wire format *)
    } in
    match Transport.send conn (Message_types.CancelOrder cancel) with
    | Result.Error e -> Result.Error (Send_error e)
    | Ok _ -> Ok ()

(** Send flush command *)
let send_flush (client : client) : (unit, client_error) result =
  match client.connection with
  | None -> Result.Error Not_connected
  | Some conn ->
    match Transport.send conn Message_types.Flush with
    | Result.Error e -> Result.Error (Send_error e)
    | Ok _ -> Ok ()

(** Send raw input message *)
let send_raw (client : client) (msg : Message_types.input_msg) 
    : (unit, client_error) result =
  match client.connection with
  | None -> Result.Error Not_connected
  | Some conn ->
    match Transport.send conn msg with
    | Result.Error e -> Result.Error (Send_error e)
    | Ok _ -> Ok ()

(** ============================================================================
    Receiving Messages
    ============================================================================ *)

(** Receive one message *)
let recv ?(timeout = 5.0) (client : client) 
    : (Message_types.output_msg, client_error) result =
  match client.connection with
  | None -> Result.Error Not_connected
  | Some conn ->
    match Transport.recv ~timeout conn with
    | Result.Error e -> Result.Error (Recv_error e)
    | Ok msg -> Ok msg

(** Try to receive (non-blocking) *)
let try_recv (client : client) 
    : (Message_types.output_msg option, client_error) result =
  match client.connection with
  | None -> Result.Error Not_connected
  | Some conn ->
    match Transport.try_recv conn with
    | Result.Error e -> Result.Error (Recv_error e)
    | Ok msg_opt -> Ok msg_opt

(** Receive all available messages within timeout *)
let recv_all ?(timeout = 1.0) (client : client) 
    : (Message_types.output_msg list, client_error) result =
  match client.connection with
  | None -> Result.Error Not_connected
  | Some conn ->
    match Transport.recv_batch ~timeout conn with
    | Result.Error Transport.Timeout -> Ok []
    | Result.Error e -> Result.Error (Recv_error e)
    | Ok msgs -> Ok msgs

(** ============================================================================
    High-Level Operations
    ============================================================================ *)

(** Place order and wait for response *)
let place_order (client : client)
    ~symbol ~price ~quantity ~side ?order_id ()
    : (int32 * Message_types.output_msg list, client_error) result =
  match send_order client ~symbol ~price ~quantity ~side ?order_id () with
  | Result.Error e -> Result.Error e
  | Ok oid ->
    match recv_all ~timeout:1.0 client with
    | Result.Error e -> Result.Error e
    | Ok msgs -> Ok (oid, msgs)

(** Cancel order and wait for response *)
let cancel_order (client : client) ~order_id ()
    : (Message_types.output_msg list, client_error) result =
  match send_cancel client ~order_id () with
  | Result.Error e -> Result.Error e
  | Ok () -> recv_all ~timeout:1.0 client

(** Flush all orders and wait for response *)
let flush_orders (client : client)
    : (Message_types.output_msg list, client_error) result =
  match send_flush client with
  | Result.Error e -> Result.Error e
  | Ok () -> recv_all ~timeout:2.0 client

(** Convenience: place buy order *)
let buy (client : client) ~symbol ~price ~quantity ?order_id ()
    : (int32 * Message_types.output_msg list, client_error) result =
  place_order client ~symbol ~price ~quantity ~side:Message_types.Buy ?order_id ()

(** Convenience: place sell order *)
let sell (client : client) ~symbol ~price ~quantity ?order_id ()
    : (int32 * Message_types.output_msg list, client_error) result =
  place_order client ~symbol ~price ~quantity ~side:Message_types.Sell ?order_id ()

(** ============================================================================
    Client Info and State
    ============================================================================ *)

let info (client : client) : string =
  match client.connection with
  | None -> "Not connected"
  | Some conn -> Transport.info conn

let is_connected (client : client) : bool =
  client.connected

let set_user_id (client : client) (user_id : int32) : unit =
  client.user_id <- user_id

let get_user_id (client : client) : int32 =
  client.user_id

let get_connection (client : client) : Transport.connection option =
  client.connection

(** ============================================================================
    Response Filtering Helpers
    ============================================================================ *)

let filter_acks (msgs : Message_types.output_msg list) : Message_types.ack list =
  List.filter_map (function Message_types.Ack a -> Some a | _ -> None) msgs

let filter_trades (msgs : Message_types.output_msg list) : Message_types.trade list =
  List.filter_map (function Message_types.Trade t -> Some t | _ -> None) msgs

let filter_tob (msgs : Message_types.output_msg list) : Message_types.top_of_book list =
  List.filter_map (function Message_types.TopOfBook t -> Some t | _ -> None) msgs

let filter_cancel_acks (msgs : Message_types.output_msg list) : Message_types.cancel_ack list =
  List.filter_map (function Message_types.CancelAck c -> Some c | _ -> None) msgs

let has_error (msgs : Message_types.output_msg list) : bool =
  List.exists (function Message_types.Error _ -> true | _ -> false) msgs

let get_error (msgs : Message_types.output_msg list) : string option =
  List.find_map (function Message_types.Error e -> Some e.error_text | _ -> None) msgs
