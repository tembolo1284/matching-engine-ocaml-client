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
    ============================================================================ *)

type scenario = {
  id: int;
  name: string;
  description: string;
  commands: Message_types.input_msg list;
}

let make_order ~user_id ~symbol ~price ~qty ~side ~order_id =
  Message_types.NewOrder {
    user_id;
    user_order_id = order_id;
    price;
    quantity = qty;
    side;
    symbol = Message_types.Symbol.of_string_exn symbol;
  }

let scenarios : scenario list = [
  {
    id = 1;
    name = "Balanced Book";
    description = "Build order book with no trades (prices don't cross)";
    commands = [
      make_order ~user_id:1l ~symbol:"IBM" ~price:9900l ~qty:50l 
        ~side:Message_types.Buy ~order_id:1l;
      make_order ~user_id:1l ~symbol:"IBM" ~price:10100l ~qty:50l 
        ~side:Message_types.Sell ~order_id:2l;
      make_order ~user_id:1l ~symbol:"IBM" ~price:9800l ~qty:100l 
        ~side:Message_types.Buy ~order_id:3l;
      make_order ~user_id:1l ~symbol:"IBM" ~price:10200l ~qty:100l 
        ~side:Message_types.Sell ~order_id:4l;
    ];
  };
  {
    id = 2;
    name = "Simple Trade";
    description = "Two orders that cross and trade";
    commands = [
      make_order ~user_id:1l ~symbol:"IBM" ~price:10000l ~qty:50l 
        ~side:Message_types.Buy ~order_id:1l;
      make_order ~user_id:2l ~symbol:"IBM" ~price:10000l ~qty:50l 
        ~side:Message_types.Sell ~order_id:1l;
    ];
  };
  {
    id = 3;
    name = "Partial Fill";
    description = "Large order partially filled by smaller order";
    commands = [
      make_order ~user_id:1l ~symbol:"AAPL" ~price:15000l ~qty:100l 
        ~side:Message_types.Buy ~order_id:1l;
      make_order ~user_id:2l ~symbol:"AAPL" ~price:15000l ~qty:30l 
        ~side:Message_types.Sell ~order_id:1l;
    ];
  };
  {
    id = 4;
    name = "Price-Time Priority";
    description = "Multiple orders, best price wins";
    commands = [
      make_order ~user_id:1l ~symbol:"MSFT" ~price:20000l ~qty:50l 
        ~side:Message_types.Buy ~order_id:1l;
      make_order ~user_id:2l ~symbol:"MSFT" ~price:20100l ~qty:50l 
        ~side:Message_types.Buy ~order_id:1l;
      make_order ~user_id:3l ~symbol:"MSFT" ~price:20100l ~qty:100l 
        ~side:Message_types.Sell ~order_id:1l;
    ];
  };
  {
    id = 5;
    name = "Multiple Symbols";
    description = "Orders for different symbols (tests dual-processor routing)";
    commands = [
      make_order ~user_id:1l ~symbol:"AAPL" ~price:15000l ~qty:50l 
        ~side:Message_types.Buy ~order_id:1l;
      make_order ~user_id:1l ~symbol:"ZZZZ" ~price:1000l ~qty:100l 
        ~side:Message_types.Buy ~order_id:2l;
      make_order ~user_id:2l ~symbol:"AAPL" ~price:15000l ~qty:50l 
        ~side:Message_types.Sell ~order_id:1l;
      make_order ~user_id:2l ~symbol:"ZZZZ" ~price:1000l ~qty:100l 
        ~side:Message_types.Sell ~order_id:2l;
    ];
  };
  {
    id = 6;
    name = "Cancel Order";
    description = "Place order then cancel it";
    commands = [
      make_order ~user_id:1l ~symbol:"IBM" ~price:10000l ~qty:50l 
        ~side:Message_types.Buy ~order_id:99l;
      Message_types.CancelOrder {
        cancel_user_id = 1l;
        cancel_user_order_id = 99l;
        cancel_symbol = Message_types.Symbol.of_string_exn "IBM";
      };
    ];
  };
]

let get_scenario (id : int) : scenario option =
  List.find_opt (fun s -> s.id = id) scenarios

let list_scenarios () : unit =
  print_endline "Available scenarios:";
  List.iter (fun s ->
    Printf.printf "  %d. %s - %s\n" s.id s.name s.description
  ) scenarios

(** ============================================================================
    Scenario Execution
    ============================================================================ *)

let count_trades (msgs : Message_types.output_msg list) : int =
  List.length (List.filter (function Message_types.Trade _ -> true | _ -> false) msgs)

let count_errors (msgs : Message_types.output_msg list) : int =
  List.length (List.filter (function Message_types.Error _ -> true | _ -> false) msgs)

let get_error_text (msgs : Message_types.output_msg list) : string option =
  List.find_map (function 
    | Message_types.Error e -> Some e.error_text 
    | _ -> None
  ) msgs

let run_scenario (client : Engine_client.client) (scenario : scenario) 
    : (scenario_result, batch_error) result =
  let start = Unix.gettimeofday () in
  
  let rec execute_all cmds responses =
    match cmds with
    | [] ->
      let elapsed = (Unix.gettimeofday () -. start) *. 1000.0 in
      Ok {
        name = scenario.name;
        commands_sent = List.length scenario.commands;
        responses_received = List.length responses;
        trades = count_trades responses;
        errors = count_errors responses;
        elapsed_ms = elapsed;
      }
    | cmd :: rest ->
      match Engine_client.send_raw client cmd with
      | Result.Error e ->
        Result.Error (Execution_error (Engine_client.client_error_to_string e))
      | Ok () ->
        match Engine_client.recv_all ~timeout:1.0 client with
        | Result.Error e ->
          Result.Error (Execution_error (Engine_client.client_error_to_string e))
        | Ok msgs ->
          execute_all rest (responses @ msgs)
  in
  execute_all scenario.commands []

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
      commands;
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

let generate_load_test ~symbol ~count () : Message_types.input_msg list =
  let sym = Message_types.Symbol.of_string_exn symbol in
  List.init count (fun i ->
    Message_types.NewOrder {
      user_id = 1l;
      user_order_id = Int32.of_int (i + 1);
      price = Int32.of_int (10000 + (i mod 100));
      quantity = 50l;
      side = if i mod 2 = 0 then Message_types.Buy else Message_types.Sell;
      symbol = sym;
    }
  )

let run_load_test (client : Engine_client.client) ~symbol ~count ()
    : fire_forget_stats =
  let msgs = generate_load_test ~symbol ~count () in
  fire_and_forget client msgs

(** ============================================================================
    Result Formatting
    ============================================================================ *)

let format_result (result : scenario_result) : string =
  Printf.sprintf
    "Scenario: %s\n\
     ========================================\n\
     Status: %s\n\
     Commands sent: %d\n\
     Responses received: %d\n\
     Trades executed: %d\n\
     Elapsed: %.2f ms"
    result.name
    (if result.errors = 0 then "PASS" else "FAIL")
    result.commands_sent
    result.responses_received
    result.trades
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
    Printf.printf "Running scenario %d: %s...\n%!" scenario.id scenario.name;
    match run_scenario client scenario with
    | Result.Error e ->
      Printf.printf "ERROR: %s\n\n" (batch_error_to_string e);
      incr failed
    | Ok result ->
      print_result result;
      if result.errors = 0 then incr passed else incr failed;
      match Engine_client.flush_orders client with
      | _ -> ()
  ) scenarios;
  
  Printf.printf "========================================\n";
  Printf.printf "Total: %d passed, %d failed\n" !passed !failed;
  (!passed, !failed)
