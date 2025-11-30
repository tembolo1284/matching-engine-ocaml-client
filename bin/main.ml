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
    
    -s, --scenario N        Run scenario N (1-6)
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
    Mode Implementations
    ============================================================================ *)

(** Run discovery and print results *)
let run_discover_only (args : cli_args) : unit =
  Printf.printf "Discovering server at %s:%d...\n%!" args.host args.port;
  
  match Discovery.discover ~host:args.host ~port:args.port () with
  | Error e ->
    Printf.printf "Discovery failed: %s\n" (Discovery.discovery_error_to_string e);
    exit 1
  | Ok (result, sock) ->
    Unix.close sock;
    Printf.printf "Discovered:\n";
    Printf.printf "  Transport: %s\n" (Message_types.transport_to_string result.transport);
    Printf.printf "  Protocol: %s\n" (Message_types.protocol_to_string result.protocol);
    Printf.printf "  Host: %s\n" result.host;
    Printf.printf "  Port: %d\n" result.port

(** Run multicast subscriber *)
let run_multicast (group : string) (port : int) (verbose : bool) : unit =
  Printf.printf "Subscribing to multicast %s:%d...\n%!" group port;
  
  match Multicast.create ~group ~port () with
  | Error e ->
    Printf.printf "Failed to create subscriber: %s\n" 
      (Multicast.subscribe_error_to_string e);
    exit 1
  | Ok sub ->
    match Multicast.subscribe sub with
    | Error e ->
      Printf.printf "Failed to subscribe: %s\n"
        (Multicast.subscribe_error_to_string e);
      exit 1
    | Ok () ->
      Printf.printf "Subscribed! Waiting for market data (Ctrl+C to stop)...\n\n%!";
      
      (* Set up signal handler for clean exit *)
      let running = ref true in
      Sys.set_signal Sys.sigint (Sys.Signal_handle (fun _ ->
        running := false;
        print_newline ()
      ));
      
      (* Receive loop *)
      while !running do
        match Multicast.recv ~timeout:1.0 sub with
        | Ok msg ->
          if verbose then
            Printf.printf "%s\n%!" (Message_types.output_msg_to_string_verbose msg)
          else
            Printf.printf "%s\n%!" (Message_types.output_msg_to_csv msg)
        | Error Multicast.Recv_timeout ->
          ()  (* Normal, keep waiting *)
        | Error e ->
          Printf.printf "Error: %s\n%!" (Multicast.recv_error_to_string e)
      done;
      
      (* Print stats and cleanup *)
      print_newline ();
      print_endline (Multicast.stats_to_string sub);
      ignore (Multicast.unsubscribe sub)

(** Create and connect client *)
let create_client (args : cli_args) : Engine_client.client option =
  let config = {
    Engine_client.host = args.host;
    port = args.port;
    transport = args.transport;
    protocol = args.protocol;
    timeout = 5.0;
    verbose = args.verbose;
  } in
  
  let client = Engine_client.create config in
  
  (* Set user ID if specified *)
  begin match args.user_id with
  | Some id -> Engine_client.set_user_id client id
  | None -> ()
  end;
  
  (* Connect *)
  match Engine_client.connect client with
  | Error e ->
    Printf.printf "Connection failed: %s\n" 
      (Engine_client.client_error_to_string e);
    None
  | Ok () ->
    if args.verbose then
      Printf.printf "Connected: %s\n%!" (Engine_client.info client);
    Some client

(** Run scenario mode *)
let run_scenario (args : cli_args) (n : int) : unit =
  match create_client args with
  | None -> exit 1
  | Some client ->
    if not (Batch.run_and_print client n) then
      exit 1;
    Engine_client.disconnect client

(** Run file mode *)
let run_file (args : cli_args) (path : string) : unit =
  match create_client args with
  | None -> exit 1
  | Some client ->
    begin match Batch.run_file client path with
    | Error e ->
      Printf.printf "Error: %s\n" (Batch.batch_error_to_string e);
      Engine_client.disconnect client;
      exit 1
    | Ok result ->
      Batch.print_result result;
      Engine_client.disconnect client;
      if List.length result.errors > 0 then exit 1
    end

(** Run load test mode *)
let run_load_test (args : cli_args) (count : int) (symbol : string) : unit =
  match create_client args with
  | None -> exit 1
  | Some client ->
    begin match Batch.run_load_test client ~num_orders:count ~symbol with
    | Error e ->
      Printf.printf "Error: %s\n" (Batch.batch_error_to_string e);
      Engine_client.disconnect client;
      exit 1
    | Ok stats ->
      Batch.print_fire_forget_stats stats;
      Engine_client.disconnect client
    end

(** Run interactive mode *)
let run_interactive (args : cli_args) : unit =
  match create_client args with
  | None -> exit 1
  | Some client ->
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
