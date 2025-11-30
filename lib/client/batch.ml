(* lib/client/batch.ml *)

(** Batch/scenario mode for automated testing.
    
    Runs predefined scenarios or reads commands from a file.
    Matches the scenario functionality of the C tcp_client.
*)

open Message_types

(** ============================================================================
    Types
    ============================================================================ *)

type scenario_result = {
  name: string;
  commands_sent: int;
  responses_received: int;
  trades: int;
  errors: string list;
  elapsed_ms: float;
}

type batch_error =
  | File_not_found of string
  | Parse_error of { line_num: int; error: string }
  | Execution_error of string

let batch_error_to_string = function
  | File_not_found path -> Printf.sprintf "File not found: %s" path
  | Parse_error { line_num; error } ->
    Printf.sprintf "Parse error on line %d: %s" line_num error
  | Execution_error msg -> Printf.sprintf "Execution error: %s" msg

(** ============================================================================
    Predefined Scenarios (match C client scenarios)
    ============================================================================ *)

(** Scenario 1: Balanced book - no trades *)
let scenario_1 client =
  let sym = Symbol.of_string_exn "IBM" in
  let cmds = [
    make_new_order ~user_id:1l ~symbol:sym ~price:100l ~quantity:50l ~side:Buy ~order_id:1l;
    make_new_order ~user_id:1l ~symbol:sym ~price:99l ~quantity:50l ~side:Buy ~order_id:2l;
    make_new_order ~user_id:2l ~symbol:sym ~price:101l ~quantity:50l ~side:Sell ~order_id:1l;
    make_new_order ~user_id:2l ~symbol:sym ~price:102l ~quantity:50l ~side:Sell ~order_id:2l;
    make_flush ();
  ] in
  ("Scenario 1: Balanced Book (no trades)", cmds)

(** Scenario 2: Simple trade *)
let scenario_2 client =
  let sym = Symbol.of_string_exn "IBM" in
  let cmds = [
    make_new_order ~user_id:1l ~symbol:sym ~price:100l ~quantity:50l ~side:Buy ~order_id:1l;
    make_new_order ~user_id:2l ~symbol:sym ~price:100l ~quantity:50l ~side:Sell ~order_id:1l;
    make_flush ();
  ] in
  ("Scenario 2: Simple Trade", cmds)

(** Scenario 3: Partial fill *)
let scenario_3 client =
  let sym = Symbol.of_string_exn "IBM" in
  let cmds = [
    make_new_order ~user_id:1l ~symbol:sym ~price:100l ~quantity:100l ~side:Buy ~order_id:1l;
    make_new_order ~user_id:2l ~symbol:sym ~price:100l ~quantity:30l ~side:Sell ~order_id:1l;
    make_new_order ~user_id:2l ~symbol:sym ~price:100l ~quantity:30l ~side:Sell ~order_id:2l;
    make_flush ();
  ] in
  ("Scenario 3: Partial Fill", cmds)

(** Scenario 4: Price-time priority *)
let scenario_4 client =
  let sym = Symbol.of_string_exn "IBM" in
  let cmds = [
    make_new_order ~user_id:1l ~symbol:sym ~price:100l ~quantity:50l ~side:Buy ~order_id:1l;
    make_new_order ~user_id:2l ~symbol:sym ~price:100l ~quantity:50l ~side:Buy ~order_id:1l;
    make_new_order ~user_id:3l ~symbol:sym ~price:100l ~quantity:75l ~side:Sell ~order_id:1l;
    make_flush ();
  ] in
  ("Scenario 4: Price-Time Priority", cmds)

(** Scenario 5: Multiple symbols (tests dual-processor routing) *)
let scenario_5 client =
  let ibm = Symbol.of_string_exn "IBM" in   (* A-M -> Processor 0 *)
  let nvda = Symbol.of_string_exn "NVDA" in (* N-Z -> Processor 1 *)
  let cmds = [
    make_new_order ~user_id:1l ~symbol:ibm ~price:100l ~quantity:50l ~side:Buy ~order_id:1l;
    make_new_order ~user_id:1l ~symbol:nvda ~price:200l ~quantity:25l ~side:Buy ~order_id:2l;
    make_new_order ~user_id:2l ~symbol:ibm ~price:100l ~quantity:50l ~side:Sell ~order_id:1l;
    make_new_order ~user_id:2l ~symbol:nvda ~price:200l ~quantity:25l ~side:Sell ~order_id:2l;
    make_flush ();
  ] in
  ("Scenario 5: Multiple Symbols (Dual-Processor)", cmds)

(** Scenario 6: Cancel order *)
let scenario_6 client =
  let sym = Symbol.of_string_exn "IBM" in
  let cmds = [
    make_new_order ~user_id:1l ~symbol:sym ~price:100l ~quantity:50l ~side:Buy ~order_id:1l;
    make_cancel ~user_id:1l ~symbol:sym ~order_id:1l;
    make_flush ();
  ] in
  ("Scenario 6: Cancel Order", cmds)

(** All predefined scenarios *)
let scenarios = [|
  scenario_1;
  scenario_2;
  scenario_3;
  scenario_4;
  scenario_5;
  scenario_6;
|]

let num_scenarios = Array.length scenarios

(** ============================================================================
    Scenario Execution
    ============================================================================ *)

(** Execute a list of commands and collect responses *)
let execute_commands 
    (client : Engine_client.client) 
    (commands : input_msg list) 
    : (output_msg list, batch_error) result =
  
  (* Send all commands *)
  let rec send_all = function
    | [] -> Ok ()
    | cmd :: rest ->
      match client.connection with
      | None -> Error (Execution_error "Not connected")
      | Some conn ->
        match Transport.send conn cmd with
        | Error e -> Error (Execution_error (Transport.send_error_to_string e))
        | Ok _ -> send_all rest
  in
  
  match send_all commands with
  | Error e -> Error e
  | Ok () ->
    (* Collect all responses *)
    match Engine_client.recv_all ~timeout:3.0 client with
    | Error e -> Error (Execution_error (Engine_client.client_error_to_string e))
    | Ok msgs -> Ok msgs

(** Run a single scenario *)
let run_scenario 
    (client : Engine_client.client) 
    (scenario_num : int) 
    : (scenario_result, batch_error) result =
  
  if scenario_num < 1 || scenario_num > num_scenarios then
    Error (Execution_error (Printf.sprintf "Invalid scenario: %d (1-%d available)" 
                              scenario_num num_scenarios))
  else
    let scenario_fn = scenarios.(scenario_num - 1) in
    let (name, commands) = scenario_fn client in
    
    let start_time = Unix.gettimeofday () in
    
    match execute_commands client commands with
    | Error e -> Error e
    | Ok responses ->
      let elapsed_ms = (Unix.gettimeofday () -. start_time) *. 1000.0 in
      
      (* Count trades *)
      let trades = List.length (Engine_client.filter_trades responses) in
      
      (* Collect errors *)
      let errors = List.filter_map (function
        | Error e -> Some e.error_text
        | _ -> None
      ) responses in
      
      Ok {
        name;
        commands_sent = List.length commands;
        responses_received = List.length responses;
        trades;
        errors;
        elapsed_ms;
      }

(** ============================================================================
    File-Based Batch Execution
    ============================================================================ *)

(** Parse a command file (one command per line) *)
let parse_command_file (path : string) : (input_msg list, batch_error) result =
  if not (Sys.file_exists path) then
    Error (File_not_found path)
  else
    let ic = open_in path in
    let rec read_lines acc line_num =
      match input_line ic with
      | line ->
        let line = String.trim line in
        if String.length line = 0 || line.[0] = '#' then
          (* Skip empty lines and comments *)
          read_lines acc (line_num + 1)
        else
          begin match Csv_codec.parse_input_msg line with
          | Error e ->
            close_in ic;
            Error (Parse_error { line_num; error = Csv_codec.parse_error_to_string e })
          | Ok cmd ->
            read_lines (cmd :: acc) (line_num + 1)
          end
      | exception End_of_file ->
        close_in ic;
        Ok (List.rev acc)
    in
    read_lines [] 1

(** Run commands from a file *)
let run_file 
    (client : Engine_client.client) 
    (path : string) 
    : (scenario_result, batch_error) result =
  
  match parse_command_file path with
  | Error e -> Error e
  | Ok commands ->
    let start_time = Unix.gettimeofday () in
    
    match execute_commands client commands with
    | Error e -> Error e
    | Ok responses ->
      let elapsed_ms = (Unix.gettimeofday () -. start_time) *. 1000.0 in
      let trades = List.length (Engine_client.filter_trades responses) in
      let errors = List.filter_map (function
        | Error e -> Some e.error_text
        | _ -> None
      ) responses in
      
      Ok {
        name = Printf.sprintf "File: %s" path;
        commands_sent = List.length commands;
        responses_received = List.length responses;
        trades;
        errors;
        elapsed_ms;
      }

(** ============================================================================
    Output Formatting
    ============================================================================ *)

(** Format scenario result *)
let format_result (result : scenario_result) : string =
  let status = if List.length result.errors = 0 then "PASS" else "FAIL" in
  Printf.sprintf
    "%s\n\
     ========================================\n\
     Status: %s\n\
     Commands sent: %d\n\
     Responses received: %d\n\
     Trades executed: %d\n\
     Elapsed: %.2f ms\n\
     %s"
    result.name
    status
    result.commands_sent
    result.responses_received
    result.trades
    result.elapsed_ms
    (if List.length result.errors > 0 then
       "Errors:\n" ^ String.concat "\n" (List.map (fun e -> "  - " ^ e) result.errors)
     else "")

(** Print result to stdout *)
let print_result (result : scenario_result) : unit =
  print_endline (format_result result);
  print_newline ()

(** ============================================================================
    Convenience Functions
    ============================================================================ *)

(** Run scenario and print result *)
let run_and_print (client : Engine_client.client) (scenario_num : int) : bool =
  Printf.printf "Running scenario %d...\n%!" scenario_num;
  match run_scenario client scenario_num with
  | Error e ->
    Printf.printf "Error: %s\n%!" (batch_error_to_string e);
    false
  | Ok result ->
    print_result result;
    List.length result.errors = 0

(** Run all scenarios *)
let run_all (client : Engine_client.client) : int * int =
  let passed = ref 0 in
  let failed = ref 0 in
  
  for i = 1 to num_scenarios do
    if run_and_print client i then
      incr passed
    else
      incr failed
  done;
  
  Printf.printf "\n========================================\n";
  Printf.printf "Results: %d passed, %d failed\n%!" !passed !failed;
  
  (!passed, !failed)

(** List available scenarios *)
let list_scenarios () : unit =
  print_endline "Available scenarios:";
  for i = 1 to num_scenarios do
    let (name, _) = scenarios.(i - 1) (Obj.magic ()) in  (* Dummy client just to get name *)
    Printf.printf "  %d. %s\n" i name
  done
```

