(* bin/main.ml *)

(** Matching Engine OCaml Client - CLI Entry Point

    Usage:
      me_client [OPTIONS]

    Modes:
      - Interactive (default): REPL for manual trading
      - Scenario: Run predefined test scenarios
      - File: Execute commands from a file
      - Load test: Fire-and-forget throughput testing
      - Multicast: Subscribe to market data feed
*)

open Matching_engine_client

(** ============================================================================
    CLI Argument Types
    ============================================================================ *)

type mode =
  | Mode_interactive
  | Mode_scenario of int
  | Mode_file of string
  | Mode_load_test of int * string  (* count, symbol *)
  | Mode_multicast of string * int  (* group, port *)
  | Mode_list_scenarios
  | Mode_discover_only

type cli_args = {
  host: string;
  port: int;
  mode: mode;
  transport: Message_types.transport option;
  protocol: Message_types.protocol option;
  verbose: bool;
  user_id: int32 option;
}

(** ============================================================================
    Argument Parsing
    ============================================================================ *)

let default_args = {
  host = "localhost";
  port = 1234;
  mode = Mode_interactive;
  transport = None;
  protocol = None;
  verbose = false;
  user_id = None;
}

let usage_msg = {|
Matching Engine OCaml Client

USAGE:
    me_client [OPTIONS]

OPTIONS:
    -h, --host HOST         Server hostname (default: localhost)
    -p, --port PORT         Server port (default: 1234)

    --tcp                   Force TCP transport
    --udp                   Force UDP transport
    --csv                   Force CSV protocol
    --binary                Force binary protocol

    -s, --scenario N        Run scenario N
    -f, --file PATH         Execute commands from file
    -l, --load-test N       Fire-and-forget N orders
    --symbol SYM            Symbol for load test (default: IBM)

    -m, --multicast ADDR    Subscribe to multicast (e.g., 239.255.0.1:5000)

    --discover              Discover server and exit
    --list-scenarios        List available scenarios and exit

    -u, --user-id ID        Set user ID (default: 1)
    -v, --verbose           Enable verbose output
    --help                  Show this help

EXAMPLES:
    # Interactive mode with auto-discovery
    me_client -h localhost -p 1234

    # Run scenario 2
    me_client -h localhost -p 1234 -s 2

    # Force TCP + Binary
    me_client -h localhost -p 1234 --tcp --binary

    # Load test with 10000 orders
    me_client -h localhost -p 1234 -l 10000 --symbol AAPL

    # Subscribe to multicast market data
    me_client -m 239.255.0.1:5000

|}

(** Parse multicast address "group:port" *)
let parse_multicast_addr (s : string) : (string * int) option =
  match String.split_on_char ':' s with
  | [group; port_str] ->
    begin match int_of_string_opt port_str with
    | Some port -> Some (group, port)
    | None -> None
    end
  | _ -> None

(** Parse command-line arguments *)
let parse_args () : cli_args =
  let args = ref default_args in
  let load_test_symbol = ref "IBM" in

  let rec parse argv =
    match argv with
    | [] -> ()

    (* Host and port *)
    | ("-h" | "--host") :: host :: rest ->
      args := { !args with host };
      parse rest
    | ("-p" | "--port") :: port_str :: rest ->
      begin match int_of_string_opt port_str with
      | Some port -> args := { !args with port }
      | None ->
        Printf.eprintf "Invalid port: %s\n" port_str;
        exit 1
      end;
      parse rest

    (* Transport *)
    | "--tcp" :: rest ->
      args := { !args with transport = Some Message_types.TCP };
      parse rest
    | "--udp" :: rest ->
      args := { !args with transport = Some Message_types.UDP };
      parse rest

    (* Protocol *)
    | "--csv" :: rest ->
      args := { !args with protocol = Some Message_types.CSV };
      parse rest
    | "--binary" :: rest ->
      args := { !args with protocol = Some Message_types.Binary };
      parse rest

    (* Modes *)
    | ("-s" | "--scenario") :: n_str :: rest ->
      begin match int_of_string_opt n_str with
      | Some n -> args := { !args with mode = Mode_scenario n }
      | None ->
        Printf.eprintf "Invalid scenario number: %s\n" n_str;
        exit 1
      end;
      parse rest

    | ("-f" | "--file") :: path :: rest ->
      args := { !args with mode = Mode_file path };
      parse rest

    | ("-l" | "--load-test") :: n_str :: rest ->
      begin match int_of_string_opt n_str with
      | Some n ->
        args := { !args with mode = Mode_load_test (n, !load_test_symbol) }
      | None ->
        Printf.eprintf "Invalid load test count: %s\n" n_str;
        exit 1
      end;
      parse rest

    | "--symbol" :: sym :: rest ->
      load_test_symbol := sym;
      (* Update load test mode if already set *)
      begin match !args.mode with
      | Mode_load_test (n, _) ->
        args := { !args with mode = Mode_load_test (n, sym) }
      | _ -> ()
      end;
      parse rest

    | ("-m" | "--multicast") :: addr :: rest ->
      begin match parse_multicast_addr addr with
      | Some (group, port) ->
        args := { !args with mode = Mode_multicast (group, port) }
      | None ->
        Printf.eprintf "Invalid multicast address: %s (expected group:port)\n" addr;
        exit 1
      end;
      parse rest

    | "--discover" :: rest ->
      args := { !args with mode = Mode_discover_only };
      parse rest

    | "--list-scenarios" :: rest ->
      args := { !args with mode = Mode_list_scenarios };
      parse rest

    (* Other options *)
    | ("-u" | "--user-id") :: id_str :: rest ->
      begin match Int32.of_string_opt id_str with
      | Some id -> args := { !args with user_id = Some id }
      | None ->
        Printf.eprintf "Invalid user ID: %s\n" id_str;
        exit 1
      end;
      parse rest

    | ("-v" | "--verbose") :: rest ->
      args := { !args with verbose = true };
      parse rest

    | "--help" :: _ ->
      print_string usage_msg;
      exit 0

    (* Unknown *)
    | arg :: _ when String.length arg > 0 && arg.[0] = '-' ->
      Printf.eprintf "Unknown option: %s\n" arg;
      Printf.eprintf "Try --help for usage.\n";
      exit 1

    | _ :: rest ->
      parse rest
  in

  (* Skip program name *)
  let argv = Array.to_list Sys.argv |> List.tl in
  parse argv;
  !args

(** ============================================================================
    Helper: Get connection info string
    ============================================================================ *)

let get_connection_info (client : Engine_client.client) (args : cli_args) 
    : string * string * bool * bool =
  match Engine_client.get_connection client with
  | None -> ("unknown", "unknown", false, false)
  | Some conn -> 
    let transport = if Transport.is_tcp conn then "TCP" else "UDP" in
    let protocol = if Transport.is_binary conn then "Binary" else "CSV" in
    let transport_auto = Option.is_none args.transport in
    let protocol_auto = Option.is_none args.protocol in
    (transport, protocol, transport_auto, protocol_auto)

let print_banner () =
  Printf.printf "===========================================\n";
  Printf.printf "  Matching Engine OCaml Client v1.0.0\n";
  Printf.printf "===========================================\n"

(** ============================================================================
    Mode Implementations
    ============================================================================ *)

(** Run discovery and print results *)
let run_discover_only (args : cli_args) : unit =
  Printf.printf "Discovering server at %s:%d...\n%!" args.host args.port;

  match Discovery.discover ~host:args.host ~port:args.port () with
  | Result.Error e ->
    Printf.printf "Discovery failed: %s\n" (Discovery.discovery_error_to_string e);
    exit 1
  | Ok (result, sock) ->
    Unix.close sock;
    Printf.printf "Discovered:\n";
    Printf.printf "  Transport: %s\n" 
      (match result.Discovery.transport with
       | Message_types.TCP -> "TCP"
       | Message_types.UDP -> "UDP");
    Printf.printf "  Protocol: %s\n"
      (match result.Discovery.protocol with
       | Message_types.Binary -> "Binary"
       | Message_types.CSV -> "CSV");
    Printf.printf "  Host: %s\n" args.host;
    Printf.printf "  Port: %d\n" args.port

(** Run multicast subscriber *)
let run_multicast (group_str : string) (port : int) (verbose : bool) : unit =
  Printf.printf "Subscribing to multicast %s:%d...\n%!" group_str port;

  let group_addr = 
    try Unix.inet_addr_of_string group_str
    with _ ->
      Printf.printf "Invalid multicast group address: %s\n" group_str;
      exit 1
  in

  let sub = Multicast.create ~group_addr ~port () in
  
  match Multicast.subscribe sub with
  | Result.Error e ->
    Printf.printf "Failed to subscribe: %s\n"
      (Multicast.subscribe_error_to_string e);
    exit 1
  | Ok () ->
    Printf.printf "Subscribed! Waiting for market data (Ctrl+C to stop)...\n\n%!";

    let running = ref true in
    Sys.set_signal Sys.sigint (Sys.Signal_handle (fun _ ->
      running := false;
      print_newline ()
    ));

    while !running do
      match Multicast.try_recv sub with
      | Ok (Some msg) ->
        if verbose then
          Printf.printf "%s\n%!" (Message_types.output_msg_to_string_verbose msg)
        else
          Printf.printf "%s\n%!" (Message_types.output_msg_to_csv msg)
      | Ok None ->
        Unix.sleepf 0.01
      | Result.Error e ->
        Printf.printf "Error: %s\n%!" (Multicast.recv_error_to_string e)
    done;

    print_newline ();
    print_endline "--- Multicast Statistics ---";
    print_endline (Multicast.stats_to_string sub);
    Multicast.unsubscribe sub

(** Create and connect client *)
let create_client (args : cli_args) : Engine_client.client option =
  let config : Engine_client.client_config = {
    host = args.host;
    port = args.port;
    transport = args.transport;
    protocol = args.protocol;
    timeout = 5.0;
    verbose = args.verbose;
  } in

  let client = Engine_client.create config in

  begin match args.user_id with
  | Some id -> Engine_client.set_user_id client id
  | None -> ()
  end;

  match Engine_client.connect client with
  | Result.Error e ->
    Printf.printf "Connection failed: %s\n"
      (Engine_client.client_error_to_string e);
    None
  | Ok () ->
    Some client

(** Run scenario mode *)
let run_scenario (args : cli_args) (n : int) : unit =
  print_banner ();
  Printf.printf "Target:     %s:%d\n" args.host args.port;
  Printf.printf "Transport:  %s\n" 
    (match args.transport with Some Message_types.TCP -> "TCP" 
     | Some Message_types.UDP -> "UDP" | None -> "auto");
  Printf.printf "Encoding:   %s\n"
    (match args.protocol with Some Message_types.Binary -> "Binary" 
     | Some Message_types.CSV -> "CSV" | None -> "auto");
  Printf.printf "Mode:       scenario\n";
  
  match Batch.get_scenario n with
  | None ->
    Printf.printf "\nUnknown scenario: %d\n\n" n;
    Batch.list_scenarios ();
    exit 1
  | Some scenario ->
    Printf.printf "Scenario:   %d - %s\n" n scenario.Batch.name;
    Printf.printf "\nConnecting to %s:%d...\n%!" args.host args.port;
    
    match create_client args with
    | None -> exit 1
    | Some client ->
      (* Show what we connected with *)
      let (transport, protocol, t_auto, p_auto) = get_connection_info client args in
      Printf.printf "Connected via %s%s\n" transport (if t_auto then " (auto-detected)" else "");
      Printf.printf "Server encoding: %s%s\n\n%!" protocol (if p_auto then " (auto-detected)" else "");
      
      match Batch.run_scenario client scenario with
      | Result.Error e ->
        Printf.printf "Error: %s\n" (Batch.batch_error_to_string e);
        Engine_client.disconnect client;
        exit 1
      | Ok result ->
        Batch.print_result result;
        Engine_client.disconnect client;
        if result.errors > 0 then exit 1

(** Run file mode *)
let run_file (args : cli_args) (path : string) : unit =
  print_banner ();
  Printf.printf "Target:     %s:%d\n" args.host args.port;
  Printf.printf "Mode:       file\n";
  Printf.printf "File:       %s\n" path;
  Printf.printf "\nConnecting to %s:%d...\n%!" args.host args.port;
  
  match create_client args with
  | None -> exit 1
  | Some client ->
    let (transport, protocol, t_auto, p_auto) = get_connection_info client args in
    Printf.printf "Connected via %s%s\n" transport (if t_auto then " (auto-detected)" else "");
    Printf.printf "Server encoding: %s%s\n\n%!" protocol (if p_auto then " (auto-detected)" else "");
    
    begin match Batch.run_file client path with
    | Result.Error e ->
      Printf.printf "Error: %s\n" (Batch.batch_error_to_string e);
      Engine_client.disconnect client;
      exit 1
    | Ok result ->
      Batch.print_result result;
      Engine_client.disconnect client;
      if result.errors > 0 then exit 1
    end

(** Run load test mode *)
let run_load_test (args : cli_args) (count : int) (symbol : string) : unit =
  print_banner ();
  Printf.printf "Target:     %s:%d\n" args.host args.port;
  Printf.printf "Mode:       load-test (fire-and-forget)\n";
  Printf.printf "Orders:     %d\n" count;
  Printf.printf "Symbol:     %s\n" symbol;
  Printf.printf "\nConnecting to %s:%d...\n%!" args.host args.port;
  
  match create_client args with
  | None -> exit 1
  | Some client ->
    let (transport, protocol, t_auto, p_auto) = get_connection_info client args in
    Printf.printf "Connected via %s%s\n" transport (if t_auto then " (auto-detected)" else "");
    Printf.printf "Server encoding: %s%s\n\n%!" protocol (if p_auto then " (auto-detected)" else "");
    
    Printf.printf "Sending %d orders (fire-and-forget)...\n%!" count;
    let stats = Batch.run_load_test client ~symbol ~count () in
    Batch.print_fire_forget_stats stats;
    Engine_client.disconnect client

(** Run interactive mode *)
let run_interactive (args : cli_args) : unit =
  print_banner ();
  Printf.printf "Target:     %s:%d\n" args.host args.port;
  Printf.printf "Mode:       interactive\n";
  Printf.printf "\nConnecting to %s:%d...\n%!" args.host args.port;
  
  match create_client args with
  | None -> exit 1
  | Some client ->
    let (transport, protocol, t_auto, p_auto) = get_connection_info client args in
    Printf.printf "Connected via %s%s\n" transport (if t_auto then " (auto-detected)" else "");
    Printf.printf "Server encoding: %s%s\n\n%!" protocol (if p_auto then " (auto-detected)" else "");
    
    Interactive.run client

(** ============================================================================
    Main Entry Point
    ============================================================================ *)

let () =
  let args = parse_args () in

  match args.mode with
  | Mode_list_scenarios ->
    Batch.list_scenarios ()

  | Mode_discover_only ->
    run_discover_only args

  | Mode_multicast (group, port) ->
    run_multicast group port args.verbose

  | Mode_scenario n ->
    run_scenario args n

  | Mode_file path ->
    run_file args path

  | Mode_load_test (count, symbol) ->
    run_load_test args count symbol

  | Mode_interactive ->
    run_interactive args
