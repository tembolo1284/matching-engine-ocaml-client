(* lib/client/batch.ml *)

(** Batch execution and load testing for the matching engine.

    Provides:
    - Predefined test scenarios
    - File-based command execution
    - Fire-and-forget load testing
*)

(** ============================================================================
    Types
    ============================================================================ *)

type scenario_result = {
  name: string;
  commands_sent: int;
  responses_received: int;
  trades: int;
  errors: int;
  elapsed_ms: float;
}

type fire_forget_stats = {
  messages_sent: int;
  send_errors: int;
  elapsed_sec: float;
  msgs_per_sec: float;
}

type batch_error =
  | File_not_found of string
  | Parse_error of { line_num: int; error: string }
  | Execution_error of string

let batch_error_to_string = function
  | File_not_found s -> Printf.sprintf "File not found: %s" s
  | Parse_error { line_num; error } ->
    Printf.sprintf "Parse error on line %d: %s" line_num error
  | Execution_error s -> Printf.sprintf "Execution error: %s" s

(** ============================================================================
    Predefined Scenarios
    
    NOTE: All scenarios use user_id from the client (not hardcoded) because
    the C server validates that each TCP connection only sends orders for
    its assigned client_id.
    ============================================================================ *)

type scenario = {
  id: int;
  name: string;
  description: string;
  make_commands: int32 -> Message_types.input_msg list;
}

let make_order ~symbol ~price ~qty ~side ~order_id user_id =
  Message_types.NewOrder {
    user_id;
    user_order_id = order_id;
    price;
    quantity = qty;
    side;
    symbol = Message_types.Symbol.of_string_exn symbol;
  }

let make_cancel ~order_id user_id =
  Message_types.CancelOrder {
    cancel_user_id = user_id;
    cancel_user_order_id = order_id;
    cancel_symbol = Message_types.Symbol.of_string_exn "?";
  }

let scenarios : scenario list = [
  {
    id = 1;
    name = "Simple orders (no match)";
    description = "Build order book with no trades (prices don't cross)";
    make_commands = fun uid -> [
      make_order ~symbol:"IBM" ~price:100l ~qty:50l 
        ~side:Message_types.Buy ~order_id:1l uid;
      make_order ~symbol:"IBM" ~price:105l ~qty:50l 
        ~side:Message_types.Sell ~order_id:2l uid;
      Message_types.Flush;
    ];
  };
  {
    id = 2;
    name = "Matching trade execution";
    description = "Two orders that cross and trade";
    make_commands = fun uid -> [
      make_order ~symbol:"IBM" ~price:100l ~qty:50l 
        ~side:Message_types.Buy ~order_id:1l uid;
      make_order ~symbol:"IBM" ~price:100l ~qty:50l 
        ~side:Message_types.Sell ~order_id:2l uid;
      Message_types.Flush;
    ];
  };
  {
    id = 3;
    name = "Cancel order";
    description = "Place order then cancel it";
    make_commands = fun uid -> [
      make_order ~symbol:"IBM" ~price:100l ~qty:50l 
        ~side:Message_types.Buy ~order_id:1l uid;
      make_cancel ~order_id:1l uid;
      Message_types.Flush;
    ];
  };
  {
    id = 4;
    name = "Partial fill";
    description = "Large order partially filled by smaller order";
    make_commands = fun uid -> [
      make_order ~symbol:"AAPL" ~price:150l ~qty:100l 
        ~side:Message_types.Buy ~order_id:1l uid;
      make_order ~symbol:"AAPL" ~price:150l ~qty:30l 
        ~side:Message_types.Sell ~order_id:2l uid;
      Message_types.Flush;
    ];
  };
  {
    id = 5;
    name = "Price-time priority";
    description = "Multiple orders at same price, first order trades first";
    make_commands = fun uid -> [
      make_order ~symbol:"MSFT" ~price:200l ~qty:50l 
        ~side:Message_types.Buy ~order_id:1l uid;
      make_order ~symbol:"MSFT" ~price:201l ~qty:50l 
        ~side:Message_types.Buy ~order_id:2l uid;
      make_order ~symbol:"MSFT" ~price:201l ~qty:100l 
        ~side:Message_types.Sell ~order_id:3l uid;
      Message_types.Flush;
    ];
  };
  {
    id = 6;
    name = "Multiple symbols (dual-processor)";
    description = "Orders for different symbols testing A-M/N-Z routing";
    make_commands = fun uid -> [
      make_order ~symbol:"AAPL" ~price:150l ~qty:50l 
        ~side:Message_types.Buy ~order_id:1l uid;
      make_order ~symbol:"TSLA" ~price:200l ~qty:100l 
        ~side:Message_types.Buy ~order_id:2l uid;
      make_order ~symbol:"AAPL" ~price:150l ~qty:50l 
        ~side:Message_types.Sell ~order_id:3l uid;
      make_order ~symbol:"TSLA" ~price:200l ~qty:100l 
        ~side:Message_types.Sell ~order_id:4l uid;
      Message_types.Flush;
    ];
  };
  (* Stress test scenarios *)
  {
    id = 10;
    name = "Stress: 1K orders";
    description = "Send 1000 orders rapidly";
    make_commands = fun uid ->
      List.init 1000 (fun i ->
        make_order ~symbol:"IBM" 
          ~price:(Int32.of_int (100 + (i mod 10)))
          ~qty:50l
          ~side:(if i mod 2 = 0 then Message_types.Buy else Message_types.Sell)
          ~order_id:(Int32.of_int (i + 1))
          uid
      ) @ [Message_types.Flush];
  };
  {
    id = 11;
    name = "Stress: 10K orders";
    description = "Send 10000 orders rapidly";
    make_commands = fun uid ->
      List.init 10000 (fun i ->
        make_order ~symbol:"IBM" 
          ~price:(Int32.of_int (100 + (i mod 10)))
          ~qty:50l
          ~side:(if i mod 2 = 0 then Message_types.Buy else Message_types.Sell)
          ~order_id:(Int32.of_int (i + 1))
          uid
      ) @ [Message_types.Flush];
  };
  (* Multi-symbol stress test *)
  {
    id = 30;
    name = "Multi-symbol: 1K orders";
    description = "1000 orders across multiple symbols (tests dual-processor)";
    make_commands = fun uid ->
      let symbols = [|"AAPL"; "IBM"; "GOOGL"; "META"; "NVDA"; "TSLA"; "UBER"; "ZM"|] in
      List.init 1000 (fun i ->
        let symbol = symbols.(i mod Array.length symbols) in
        make_order ~symbol
          ~price:(Int32.of_int (100 + (i mod 10)))
          ~qty:50l
          ~side:(if i mod 2 = 0 then Message_types.Buy else Message_types.Sell)
          ~order_id:(Int32.of_int (i + 1))
          uid
      ) @ [Message_types.Flush];
  };
]

let get_scenario (id : int) : scenario option =
  List.find_opt (fun s -> s.id = id) scenarios

let list_scenarios () : unit =
  print_endline "Available scenarios:";
  print_endline "";
  print_endline "Basic:";
  List.iter (fun s ->
    if s.id < 10 then
      Printf.printf "  %-3d - %s\n" s.id s.name
  ) scenarios;
  print_endline "";
  print_endline "Stress Tests:";
  List.iter (fun s ->
    if s.id >= 10 && s.id < 30 then
      Printf.printf "  %-3d - %s\n" s.id s.name
  ) scenarios;
  print_endline "";
  print_endline "Multi-Symbol (dual-processor):";
  List.iter (fun s ->
    if s.id >= 30 then
      Printf.printf "  %-3d - %s\n" s.id s.name
  ) scenarios

(** ============================================================================
    Output Formatting
    ============================================================================ *)

(** Format input message for display (what we're sending) *)
let format_send_msg (msg : Message_types.input_msg) : string =
  match msg with
  | Message_types.NewOrder o ->
    Printf.sprintf "%s %s %ld@%ld"
      (if o.side = Message_types.Buy then "BUY" else "SELL")
      (Message_types.Symbol.to_string o.symbol)
      o.quantity
      o.price
  | Message_types.CancelOrder c ->
    Printf.sprintf "CANCEL order %ld" c.cancel_user_order_id
  | Message_types.Flush ->
    "FLUSH"

(** Format received message for display *)
let format_recv_msg (msg : Message_types.output_msg) : string =
  Message_types.output_msg_to_csv msg

(** ============================================================================
    Scenario Execution
    ============================================================================ *)

let count_trades (msgs : Message_types.output_msg list) : int =
  List.length (List.filter (function Message_types.Trade _ -> true | _ -> false) msgs)

let count_errors (msgs : Message_types.output_msg list) : int =
  List.length (List.filter (function Message_types.Error _ -> true | _ -> false) msgs)

(** Run scenario with verbose output showing each send/receive *)
let run_scenario (client : Engine_client.client) (scenario : scenario) 
    : (scenario_result, batch_error) result =
  let start = Unix.gettimeofday () in
  let user_id = Engine_client.get_user_id client in
  let commands = scenario.make_commands user_id in
  
  Printf.printf "=== Scenario %d: %s ===\n%!" scenario.id scenario.name;
  
  let rec execute_all cmds all_responses =
    match cmds with
    | [] ->
      let elapsed = (Unix.gettimeofday () -. start) *. 1000.0 in
      Ok {
        name = scenario.name;
        commands_sent = List.length commands;
        responses_received = List.length all_responses;
        trades = count_trades all_responses;
        errors = count_errors all_responses;
        elapsed_ms = elapsed;
      }
    | cmd :: rest ->
      (* Print what we're sending *)
      Printf.printf "Sending: %s\n%!" (format_send_msg cmd);
      
      match Engine_client.send_raw client cmd with
      | Result.Error e ->
        Result.Error (Execution_error (Engine_client.client_error_to_string e))
      | Ok () ->
        (* Small delay to let server process *)
        Unix.sleepf 0.01;
        
        (* Receive and print all responses *)
        match Engine_client.recv_all ~timeout:1.0 client with
        | Result.Error e ->
          Result.Error (Execution_error (Engine_client.client_error_to_string e))
        | Ok msgs ->
          List.iter (fun msg ->
            Printf.printf "[RECV] %s\n%!" (format_recv_msg msg)
          ) msgs;
          execute_all rest (all_responses @ msgs)
  in
  execute_all commands []

(** ============================================================================
    File-based Execution
    ============================================================================ *)

let parse_command_file (filename : string) 
    : (Message_types.input_msg list, batch_error) result =
  if not (Sys.file_exists filename) then
    Result.Error (File_not_found filename)
  else
    let ic = open_in filename in
    let rec read_lines line_num acc =
      match input_line ic with
      | line ->
        let line = String.trim line in
        if String.length line = 0 || line.[0] = '#' then
          read_lines (line_num + 1) acc
        else begin
          match Csv_codec.parse_input_msg line with
          | Result.Error e ->
            close_in ic;
            Result.Error (Parse_error { 
              line_num; 
              error = Csv_codec.parse_error_to_string e 
            })
          | Ok msg ->
            read_lines (line_num + 1) (msg :: acc)
        end
      | exception End_of_file ->
        close_in ic;
        Ok (List.rev acc)
    in
    read_lines 1 []

let run_file (client : Engine_client.client) (filename : string)
    : (scenario_result, batch_error) result =
  match parse_command_file filename with
  | Result.Error e -> Result.Error e
  | Ok commands ->
    let scenario = {
      id = 0;
      name = Printf.sprintf "File: %s" filename;
      description = "";
      make_commands = fun _ -> commands;
    } in
    run_scenario client scenario

(** ============================================================================
    Fire-and-Forget Mode
    ============================================================================ *)

let fire_and_forget (client : Engine_client.client) 
    (msgs : Message_types.input_msg list) : fire_forget_stats =
  let start = Unix.gettimeofday () in
  let sent = ref 0 in
  let errors = ref 0 in
  
  List.iter (fun msg ->
    match Engine_client.send_raw client msg with
    | Result.Error _ -> incr errors
    | Ok () -> incr sent
  ) msgs;
  
  let elapsed = Unix.gettimeofday () -. start in
  {
    messages_sent = !sent;
    send_errors = !errors;
    elapsed_sec = elapsed;
    msgs_per_sec = if elapsed > 0.0 then float_of_int !sent /. elapsed else 0.0;
  }

let generate_load_test ~symbol ~count user_id : Message_types.input_msg list =
  let sym = Message_types.Symbol.of_string_exn symbol in
  List.init count (fun i ->
    Message_types.NewOrder {
      user_id;
      user_order_id = Int32.of_int (i + 1);
      price = Int32.of_int (100 + (i mod 10));
      quantity = 50l;
      side = if i mod 2 = 0 then Message_types.Buy else Message_types.Sell;
      symbol = sym;
    }
  )

let run_load_test (client : Engine_client.client) ~symbol ~count ()
    : fire_forget_stats =
  let user_id = Engine_client.get_user_id client in
  let msgs = generate_load_test ~symbol ~count user_id in
  fire_and_forget client msgs

(** ============================================================================
    Result Formatting
    ============================================================================ *)

let format_result (result : scenario_result) : string =
  Printf.sprintf
    "-------------------------------------------\n\
     Result: %s\n\
     Commands sent: %d\n\
     Responses received: %d\n\
     Trades executed: %d\n\
     Errors: %d\n\
     Elapsed: %.2f ms"
    (if result.errors = 0 then "PASS" else "FAIL")
    result.commands_sent
    result.responses_received
    result.trades
    result.errors
    result.elapsed_ms

let print_result (result : scenario_result) : unit =
  print_endline (format_result result);
  print_newline ()

let format_fire_forget_stats (stats : fire_forget_stats) : string =
  Printf.sprintf
    "Fire-and-Forget Results\n\
     =======================\n\
     Messages sent: %d\n\
     Send errors: %d\n\
     Elapsed: %.3f sec\n\
     Throughput: %.0f msg/sec"
    stats.messages_sent
    stats.send_errors
    stats.elapsed_sec
    stats.msgs_per_sec

let print_fire_forget_stats (stats : fire_forget_stats) : unit =
  print_endline (format_fire_forget_stats stats)

(** ============================================================================
    Run All Scenarios
    ============================================================================ *)

let run_all (client : Engine_client.client) : int * int =
  let passed = ref 0 in
  let failed = ref 0 in
  
  List.iter (fun scenario ->
    if scenario.id < 10 then begin
      Printf.printf "\nRunning scenario %d: %s...\n%!" scenario.id scenario.name;
      match run_scenario client scenario with
      | Result.Error e ->
        Printf.printf "ERROR: %s\n\n" (batch_error_to_string e);
        incr failed
      | Ok result ->
        print_result result;
        if result.errors = 0 then incr passed else incr failed
    end
  ) scenarios;
  
  Printf.printf "========================================\n";
  Printf.printf "Total: %d passed, %d failed\n" !passed !failed;
  (!passed, !failed)
