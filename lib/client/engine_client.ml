(* lib/client/engine_client.ml *)

(** Main matching engine client.
    
    Provides a high-level interface for interacting with the matching engine:
    - Auto-discovery of transport and protocol
    - Sending orders, cancels, and flushes
    - Receiving and processing responses
    - Callback-based response handling
*)

open Message_types

(** ============================================================================
    Client Types
    ============================================================================ *)

type client_config = {
  host: string;
  port: int;
  transport: transport option;  (* None = auto-detect *)
  protocol: protocol option;    (* None = auto-detect *)
  timeout: float;               (* Default timeout for operations *)
  verbose: bool;                (* Print debug info *)
}

type client = {
  config: client_config;
  mutable connection: Transport.connection option;
  mutable user_id: int32;       (* Assigned by server in TCP mode *)
  mutable next_order_id: int32; (* Local order ID counter *)
  mutable connected: bool;
}

type client_error =
  | Not_connected
  | Discovery_failed of Discovery.discovery_error
  | Send_error of Transport.send_error
  | Recv_error of Transport.recv_error
  | Invalid_response of string

let client_error_to_string = function
  | Not_connected -> "Not connected"
  | Discovery_failed e -> Discovery.discovery_error_to_string e
  | Send_error e -> Transport.send_error_to_string e
  | Recv_error e -> Transport.recv_error_to_string e
  | Invalid_response msg -> Printf.sprintf "Invalid response: %s" msg

(** ============================================================================
    Client Creation
    ============================================================================ *)

let default_config ~host ~port () : client_config = {
  host;
  port;
  transport = None;
  protocol = None;
  timeout = 5.0;
  verbose = false;
}

let create (config : client_config) : client = {
  config;
  connection = None;
  user_id = 1l;  (* Default, may be assigned by server *)
  next_order_id = 1l;
  connected = false;
}

(** ============================================================================
    Connection Management
    ============================================================================ *)

(** Connect to server with auto-discovery or explicit settings *)
let connect (client : client) : (unit, client_error) result =
  (* Disconnect if already connected *)
  begin match client.connection with
  | Some conn -> 
    Transport.close conn;
    client.connection <- None;
    client.connected <- false
  | None -> ()
  end;
  
  let config = client.config in
  
  (* Use explicit settings or auto-discover *)
  match config.transport, config.protocol with
  | Some transport, Some protocol ->
    (* Explicit configuration - no discovery *)
    begin match Discovery.connect_explicit ~transport ~host:config.host ~port:config.port () with
    | Error e -> Error (Discovery_failed e)
    | Ok socket ->
      let conn = Transport.create 
        ~socket ~transport ~protocol 
        ~host:config.host ~port:config.port () 
      in
      client.connection <- Some conn;
      client.connected <- true;
      if config.verbose then
        Printf.printf "[Client] Connected: %s\n%!" (Transport.connection_info conn);
      Ok ()
    end
  
  | _ ->
    (* Auto-discovery *)
    begin match Discovery.discover ~host:config.host ~port:config.port () with
    | Error e -> Error (Discovery_failed e)
    | Ok (result, socket) ->
      let conn = Transport.create
        ~socket
        ~transport:result.transport
        ~protocol:result.protocol
        ~host:config.host
        ~port:config.port
        ()
      in
      client.connection <- Some conn;
      client.connected <- true;
      if config.verbose then
        Printf.printf "[Client] Discovered and connected: %s\n%!" 
          (Transport.connection_info conn);
      Ok ()
    end

(** Disconnect from server *)
let disconnect (client : client) : unit =
  match client.connection with
  | Some conn ->
    Transport.close conn;
    client.connection <- None;
    client.connected <- false;
    if client.config.verbose then
      Printf.printf "[Client] Disconnected\n%!"
  | None -> ()

(** Check if connected *)
let is_connected (client : client) : bool =
  client.connected && Option.is_some client.connection

(** ============================================================================
    Order Operations
    ============================================================================ *)

(** Generate next order ID *)
let next_order_id (client : client) : int32 =
  let id = client.next_order_id in
  client.next_order_id <- Int32.add client.next_order_id 1l;
  id

(** Send a new order *)
let send_order 
    (client : client) 
    ~symbol 
    ~price 
    ~quantity 
    ~side 
    ?(order_id : int32 option)
    () : (int32, client_error) result =
  match client.connection with
  | None -> Error Not_connected
  | Some conn ->
    let order_id = match order_id with
      | Some id -> id
      | None -> next_order_id client
    in
    let msg = make_new_order 
      ~user_id:client.user_id
      ~symbol
      ~price
      ~quantity
      ~side
      ~order_id
    in
    if client.config.verbose then
      Printf.printf "[Client] Sending order: %s %s %ld @ %ld (id=%ld)\n%!"
        (match side with Buy -> "BUY" | Sell -> "SELL")
        (Symbol.to_string symbol)
        quantity price order_id;
    match Transport.send conn msg with
    | Ok _ -> Ok order_id
    | Error e -> Error (Send_error e)

(** Send a cancel request *)
let send_cancel 
    (client : client) 
    ~order_id 
    () : (unit, client_error) result =
  match client.connection with
  | None -> Error Not_connected
  | Some conn ->
    let msg = make_cancel 
      ~user_id:client.user_id
      ~symbol:(Symbol.of_string_exn "?")  (* Cancel doesn't need symbol on wire *)
      ~order_id
    in
    if client.config.verbose then
      Printf.printf "[Client] Sending cancel: order_id=%ld\n%!" order_id;
    match Transport.send conn msg with
    | Ok _ -> Ok ()
    | Error e -> Error (Send_error e)

(** Send a flush command *)
let send_flush (client : client) : (unit, client_error) result =
  match client.connection with
  | None -> Error Not_connected
  | Some conn ->
    let msg = make_flush () in
    if client.config.verbose then
      Printf.printf "[Client] Sending flush\n%!";
    match Transport.send conn msg with
    | Ok _ -> Ok ()
    | Error e -> Error (Send_error e)

(** ============================================================================
    Receiving Responses
    ============================================================================ *)

(** Receive one response message *)
let recv (client : client) : (output_msg, client_error) result =
  match client.connection with
  | None -> Error Not_connected
  | Some conn ->
    match Transport.recv ~timeout:client.config.timeout conn with
    | Ok msg ->
      if client.config.verbose then
        Printf.printf "[Client] Received: %s\n%!" (output_msg_to_string_verbose msg);
      Ok msg
    | Error e -> Error (Recv_error e)

(** Try to receive without blocking *)
let try_recv (client : client) : (output_msg option, client_error) result =
  match client.connection with
  | None -> Error Not_connected
  | Some conn ->
    match Transport.try_recv conn with
    | Ok msg_opt ->
      begin match msg_opt with
      | Some msg when client.config.verbose ->
        Printf.printf "[Client] Received: %s\n%!" (output_msg_to_string_verbose msg)
      | _ -> ()
      end;
      Ok msg_opt
    | Error e -> Error (Recv_error e)

(** Receive all available responses (non-blocking after first) *)
let recv_all ?(timeout=5.0) (client : client) : (output_msg list, client_error) result =
  match client.connection with
  | None -> Error Not_connected
  | Some conn ->
    match Transport.recv_batch ~timeout conn with
    | Ok msgs ->
      if client.config.verbose then
        List.iter (fun msg ->
          Printf.printf "[Client] Received: %s\n%!" (output_msg_to_string_verbose msg)
        ) msgs;
      Ok msgs
    | Error e -> Error (Recv_error e)

(** ============================================================================
    High-Level Operations (Send + Receive)
    ============================================================================ *)

(** Send order and wait for acknowledgment *)
let place_order
    (client : client)
    ~symbol
    ~price
    ~quantity
    ~side
    () : (int32 * output_msg list, client_error) result =
  match send_order client ~symbol ~price ~quantity ~side () with
  | Error e -> Error e
  | Ok order_id ->
    (* Receive responses (ack, possibly trades, TOB updates) *)
    match recv_all ~timeout:client.config.timeout client with
    | Error e -> Error e
    | Ok msgs -> Ok (order_id, msgs)

(** Cancel order and wait for acknowledgment *)
let cancel_order
    (client : client)
    ~order_id
    () : (output_msg list, client_error) result =
  match send_cancel client ~order_id () with
  | Error e -> Error e
  | Ok () -> recv_all ~timeout:client.config.timeout client

(** Flush all orders and collect cancel acks *)
let flush_orders (client : client) : (output_msg list, client_error) result =
  match send_flush client with
  | Error e -> Error e
  | Ok () -> recv_all ~timeout:client.config.timeout client

(** ============================================================================
    Convenience Helpers
    ============================================================================ *)

(** Buy order shorthand *)
let buy client ~symbol ~price ~quantity () =
  place_order client ~symbol ~price ~quantity ~side:Buy ()

(** Sell order shorthand *)
let sell client ~symbol ~price ~quantity () =
  place_order client ~symbol ~price ~quantity ~side:Sell ()

(** Set user ID (for TCP mode where server assigns ID) *)
let set_user_id client user_id =
  client.user_id <- user_id

(** Get current user ID *)
let get_user_id client = client.user_id

(** Get connection info string *)
let info client =
  match client.connection with
  | Some conn -> Transport.connection_info conn
  | None -> "Not connected"

(** ============================================================================
    Response Filtering Helpers
    ============================================================================ *)

(** Filter for acks only *)
let filter_acks msgs =
  List.filter_map (function Ack a -> Some a | _ -> None) msgs

(** Filter for trades only *)
let filter_trades msgs =
  List.filter_map (function Trade t -> Some t | _ -> None) msgs

(** Filter for TOB updates only *)
let filter_tob msgs =
  List.filter_map (function TopOfBook t -> Some t | _ -> None) msgs

(** Filter for cancel acks only *)
let filter_cancel_acks msgs =
  List.filter_map (function CancelAck c -> Some c | _ -> None) msgs

(** Check if any message is an error *)
let has_error msgs =
  List.exists (function Error _ -> true | _ -> false) msgs

(** Get first error message if any *)
let get_error msgs =
  List.find_map (function Error e -> Some e.error_text | _ -> None) msgs

