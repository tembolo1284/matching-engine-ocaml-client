(* lib/client/interactive.ml *)

(** Interactive REPL mode for the matching engine client.

    Provides a command-line interface similar to the C tcp_client:
    - buy/sell/cancel/flush commands
    - Response display with formatting
    - Help and status commands
*)

(** ============================================================================
    Types
    ============================================================================ *)

type repl_state = {
  client: Engine_client.client;
  mutable running: bool;
  mutable verbose: bool;
  mutable show_raw: bool;
  mutable prompt: string;
}

type order_cmd = {
  symbol: string;
  price: int32;
  quantity: int32;
  order_id: int32 option;
}

type command =
  | Buy of order_cmd
  | Sell of order_cmd
  | Cancel of { cancel_order_id: int32 }
  | Flush
  | Status
  | Help
  | Verbose of bool
  | Quit
  | Raw of string
  | Invalid of string

(** ============================================================================
    Command Parsing
    ============================================================================ *)

let parse_int32 s =
  match Int32.of_string_opt (String.trim s) with
  | Some n -> Ok n
  | None -> Result.Error (Printf.sprintf "Invalid number: %s" s)

let tokenize (line : string) : string list =
  let line = String.trim line in
  String.split_on_char ' ' line
  |> List.map String.trim
  |> List.filter (fun s -> String.length s > 0)

let parse_order_cmd (side : Message_types.side) (tokens : string list) : command =
  match tokens with
  | [symbol; price_s; qty_s] ->
    begin match parse_int32 price_s, parse_int32 qty_s with
    | Ok price, Ok qty ->
      let cmd = { symbol; price; quantity = qty; order_id = None } in
      if side = Message_types.Buy then Buy cmd else Sell cmd
    | Result.Error e, _ | _, Result.Error e -> Invalid e
    end
  | [symbol; price_s; qty_s; oid_s] ->
    begin match parse_int32 price_s, parse_int32 qty_s, parse_int32 oid_s with
    | Ok price, Ok qty, Ok oid ->
      let cmd = { symbol; price; quantity = qty; order_id = Some oid } in
      if side = Message_types.Buy then Buy cmd else Sell cmd
    | Result.Error e, _, _ | _, Result.Error e, _ | _, _, Result.Error e -> Invalid e
    end
  | _ ->
    Invalid "Usage: buy|sell SYMBOL PRICE QTY [ORDER_ID]"

let parse_cancel_cmd (tokens : string list) : command =
  match tokens with
  | [oid_s] ->
    begin match parse_int32 oid_s with
    | Ok oid -> Cancel { cancel_order_id = oid }
    | Result.Error e -> Invalid e
    end
  | _ ->
    Invalid "Usage: cancel ORDER_ID"

let parse_command (line : string) : command =
  let line = String.trim line in
  if String.length line = 0 then
    Invalid ""
  else if line.[0] = '/' then
    Raw (String.sub line 1 (String.length line - 1))
  else
    let tokens = tokenize line in
    match tokens with
    | [] -> Invalid ""
    | cmd :: rest ->
      match String.lowercase_ascii cmd with
      | "buy" | "b" -> parse_order_cmd Message_types.Buy rest
      | "sell" | "s" -> parse_order_cmd Message_types.Sell rest
      | "cancel" | "c" -> parse_cancel_cmd rest
      | "flush" | "f" -> Flush
      | "status" | "stat" -> Status
      | "help" | "h" | "?" -> Help
      | "verbose" | "v" ->
        begin match rest with
        | ["on"] | ["true"] | ["1"] -> Verbose true
        | ["off"] | ["false"] | ["0"] -> Verbose false
        | [] -> Verbose true
        | _ -> Invalid "Usage: verbose [on|off]"
        end
      | "quit" | "exit" | "q" -> Quit
      | "n" when List.length rest >= 5 ->
        begin match rest with
        | _uid :: symbol :: price_s :: qty_s :: side_s :: rest ->
          let side = if String.uppercase_ascii side_s = "B" 
                     then Message_types.Buy else Message_types.Sell in
          begin match parse_int32 price_s, parse_int32 qty_s with
          | Ok price, Ok qty ->
            let order_id = match rest with
              | [oid_s] -> Result.to_option (parse_int32 oid_s)
              | _ -> None
            in
            let cmd = { symbol; price; quantity = qty; order_id } in
            if side = Message_types.Buy then Buy cmd else Sell cmd
          | Result.Error e, _ | _, Result.Error e -> Invalid e
          end
        | _ -> Invalid "Usage: N userId symbol price qty side [orderId]"
        end
      | _ ->
        Invalid (Printf.sprintf "Unknown command: %s (type 'help' for commands)" cmd)

(** ============================================================================
    Output Formatting
    ============================================================================ *)

module Color = struct
  let reset = "\027[0m"
  let bold = "\027[1m"
  let red = "\027[31m"
  let green = "\027[32m"
  let yellow = "\027[33m"
  let blue = "\027[34m"
  let cyan = "\027[36m"

  let is_tty = Unix.isatty Unix.stdout

  let wrap color s =
    if is_tty then color ^ s ^ reset
    else s
end

let format_message (msg : Message_types.output_msg) : string =
  match msg with
  | Message_types.Ack a ->
    Color.wrap Color.green (
      Printf.sprintf "[ACK] %s user=%ld order=%ld"
        (Message_types.Symbol.to_string a.ack_symbol)
        a.ack_user_id
        a.ack_user_order_id
    )
  | Message_types.CancelAck c ->
    Color.wrap Color.yellow (
      Printf.sprintf "[CANCEL] %s user=%ld order=%ld"
        (Message_types.Symbol.to_string c.cancel_ack_symbol)
        c.cancel_ack_user_id
        c.cancel_ack_user_order_id
    )
  | Message_types.Trade t ->
    Color.wrap Color.cyan (
      Printf.sprintf "[TRADE] %s: BUY %ld/%ld <-> SELL %ld/%ld @ %ld x %ld"
        (Message_types.Symbol.to_string t.trade_symbol)
        t.trade_user_id_buy t.trade_user_order_id_buy
        t.trade_user_id_sell t.trade_user_order_id_sell
        t.trade_price t.trade_quantity
    )
  | Message_types.TopOfBook tob ->
    let level =
      if Message_types.tob_is_eliminated tob then
        Color.wrap Color.red "EMPTY"
      else
        Printf.sprintf "%ld @ %ld" tob.tob_quantity tob.tob_price
    in
    Color.wrap Color.blue (
      Printf.sprintf "[TOB] %s %c: %s"
        (Message_types.Symbol.to_string tob.tob_symbol)
        (Message_types.side_to_char tob.tob_side)
        level
    )
  | Message_types.Error e ->
    Color.wrap Color.red (
      Printf.sprintf "[ERROR] %s" e.error_text
    )

let print_help () =
  print_endline {|
Matching Engine Client - Interactive Mode
=========================================

Commands:
  buy SYMBOL PRICE QTY [ORDER_ID]   Place buy order
  sell SYMBOL PRICE QTY [ORDER_ID]  Place sell order
  cancel ORDER_ID                   Cancel order
  flush                             Cancel all orders
  status                            Show connection status
  verbose [on|off]                  Toggle verbose mode
  help                              Show this help
  quit                              Exit

Shortcuts:
  b  = buy
  s  = sell
  c  = cancel
  f  = flush
  q  = quit
  ?  = help

Raw CSV (prefix with /):
  /N, 1, IBM, 100, 50, B, 1         Send raw new order
  /C, 1, 1                          Send raw cancel
  /F                                Send raw flush

Examples:
  buy IBM 10000 50                  Buy 50 IBM @ $100.00
  sell AAPL 15000 100 42            Sell 100 AAPL @ $150.00, order ID 42
  cancel 42                         Cancel order 42
|}

(** ============================================================================
    Command Execution
    ============================================================================ *)

let execute_command (state : repl_state) (cmd : command) : unit =
  match cmd with
  | Buy { symbol; price; quantity; order_id } ->
    begin match Message_types.Symbol.of_string symbol with
    | None ->
      Printf.printf "%s\n%!" (Color.wrap Color.red "Invalid symbol")
    | Some sym ->
      let result = match order_id with
        | Some oid ->
          Engine_client.send_order state.client
            ~symbol:sym ~price ~quantity ~side:Message_types.Buy ~order_id:oid ()
        | None ->
          Engine_client.send_order state.client
            ~symbol:sym ~price ~quantity ~side:Message_types.Buy ()
      in
      match result with
      | Result.Error e ->
        Printf.printf "%s\n%!"
          (Color.wrap Color.red (Engine_client.client_error_to_string e))
      | Ok oid ->
        if state.verbose then
          Printf.printf "Sent BUY %s %ld @ %ld (order_id=%ld)\n%!"
            symbol quantity price oid;
        begin match Engine_client.recv_all ~timeout:1.0 state.client with
        | Result.Error e ->
          Printf.printf "%s\n%!"
            (Color.wrap Color.red (Engine_client.client_error_to_string e))
        | Ok msgs ->
          List.iter (fun m -> Printf.printf "%s\n%!" (format_message m)) msgs
        end
    end

  | Sell { symbol; price; quantity; order_id } ->
    begin match Message_types.Symbol.of_string symbol with
    | None ->
      Printf.printf "%s\n%!" (Color.wrap Color.red "Invalid symbol")
    | Some sym ->
      let result = match order_id with
        | Some oid ->
          Engine_client.send_order state.client
            ~symbol:sym ~price ~quantity ~side:Message_types.Sell ~order_id:oid ()
        | None ->
          Engine_client.send_order state.client
            ~symbol:sym ~price ~quantity ~side:Message_types.Sell ()
      in
      match result with
      | Result.Error e ->
        Printf.printf "%s\n%!"
          (Color.wrap Color.red (Engine_client.client_error_to_string e))
      | Ok oid ->
        if state.verbose then
          Printf.printf "Sent SELL %s %ld @ %ld (order_id=%ld)\n%!"
            symbol quantity price oid;
        begin match Engine_client.recv_all ~timeout:1.0 state.client with
        | Result.Error e ->
          Printf.printf "%s\n%!"
            (Color.wrap Color.red (Engine_client.client_error_to_string e))
        | Ok msgs ->
          List.iter (fun m -> Printf.printf "%s\n%!" (format_message m)) msgs
        end
    end

  | Cancel { cancel_order_id } ->
    begin match Engine_client.send_cancel state.client ~order_id:cancel_order_id () with
    | Result.Error e ->
      Printf.printf "%s\n%!"
        (Color.wrap Color.red (Engine_client.client_error_to_string e))
    | Ok () ->
      if state.verbose then
        Printf.printf "Sent CANCEL order_id=%ld\n%!" cancel_order_id;
      begin match Engine_client.recv_all ~timeout:1.0 state.client with
      | Result.Error e ->
        Printf.printf "%s\n%!"
          (Color.wrap Color.red (Engine_client.client_error_to_string e))
      | Ok msgs ->
        List.iter (fun m -> Printf.printf "%s\n%!" (format_message m)) msgs
      end
    end

  | Flush ->
    begin match Engine_client.send_flush state.client with
    | Result.Error e ->
      Printf.printf "%s\n%!"
        (Color.wrap Color.red (Engine_client.client_error_to_string e))
    | Ok () ->
      if state.verbose then
        print_endline "Sent FLUSH";
      begin match Engine_client.recv_all ~timeout:2.0 state.client with
      | Result.Error e ->
        Printf.printf "%s\n%!"
          (Color.wrap Color.red (Engine_client.client_error_to_string e))
      | Ok msgs ->
        List.iter (fun m -> Printf.printf "%s\n%!" (format_message m)) msgs;
        Printf.printf "Flushed %d orders\n%!" (List.length msgs)
      end
    end

  | Status ->
    Printf.printf "Connection: %s\n%!" (Engine_client.info state.client);
    Printf.printf "User ID: %ld\n%!" (Engine_client.get_user_id state.client);
    Printf.printf "Connected: %b\n%!" (Engine_client.is_connected state.client);
    Printf.printf "Verbose: %b\n%!" state.verbose

  | Help ->
    print_help ()

  | Verbose v ->
    state.verbose <- v;
    Printf.printf "Verbose mode: %s\n%!" (if v then "ON" else "OFF")

  | Quit ->
    state.running <- false;
    print_endline "Goodbye!"

  | Raw csv ->
    begin match Csv_codec.parse_input_msg csv with
    | Result.Error e ->
      Printf.printf "%s\n%!"
        (Color.wrap Color.red (Csv_codec.parse_error_to_string e))
    | Ok msg ->
      begin match Engine_client.get_connection state.client with
      | None ->
        Printf.printf "%s\n%!" (Color.wrap Color.red "Not connected")
      | Some conn ->
        match Transport.send conn msg with
        | Result.Error e ->
          Printf.printf "%s\n%!"
            (Color.wrap Color.red (Transport.send_error_to_string e))
        | Ok _ ->
          if state.verbose then
            Printf.printf "Sent: %s\n%!" csv;
          begin match Engine_client.recv_all ~timeout:1.0 state.client with
          | Result.Error e ->
            Printf.printf "%s\n%!"
              (Color.wrap Color.red (Engine_client.client_error_to_string e))
          | Ok msgs ->
            List.iter (fun m -> Printf.printf "%s\n%!" (format_message m)) msgs
          end
      end
    end

  | Invalid "" ->
    ()

  | Invalid msg ->
    Printf.printf "%s\n%!" (Color.wrap Color.red msg)

(** ============================================================================
    REPL Main Loop
    ============================================================================ *)

let read_line_prompt (prompt : string) : string option =
  print_string prompt;
  flush stdout;
  try Some (input_line stdin)
  with End_of_file -> None

let create_state (client : Engine_client.client) : repl_state = {
  client;
  running = true;
  verbose = false;
  show_raw = false;
  prompt = "> ";
}

let run (client : Engine_client.client) : unit =
  let state = create_state client in

  Printf.printf "\n%s\n" (Color.wrap Color.bold "Matching Engine Client");
  Printf.printf "Connected to: %s\n" (Engine_client.info client);
  Printf.printf "Type 'help' for commands, 'quit' to exit.\n\n%!";

  while state.running do
    match read_line_prompt state.prompt with
    | None ->
      state.running <- false;
      print_endline ""
    | Some line ->
      let cmd = parse_command line in
      execute_command state cmd
  done;

  Engine_client.disconnect client

(** ============================================================================
    Quick Start Helper
    ============================================================================ *)

let start ~host ~port ?transport ?protocol () : unit =
  let base_config = Engine_client.default_config ~host ~port () in
  let config = { 
    base_config with
    Engine_client.transport = transport;
    protocol = protocol;
    verbose = false;
  } in
  let client = Engine_client.create config in
  match Engine_client.connect client with
  | Result.Error e ->
    Printf.printf "Connection failed: %s\n%!"
      (Engine_client.client_error_to_string e)
  | Ok () ->
    run client
