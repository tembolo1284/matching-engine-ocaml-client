(* test/test_protocol.ml*)

(** Unit tests for protocol codecs *)

open Matching_engine_client

(** ============================================================================
    Test Helpers
    ============================================================================ *)

let test_count = ref 0
let pass_count = ref 0
let fail_count = ref 0

let test name f =
  incr test_count;
  Printf.printf "  %s... %!" name;
  try
    f ();
    incr pass_count;
    print_endline "OK"
  with e ->
    incr fail_count;
    Printf.printf "FAIL: %s\n" (Printexc.to_string e)

let assert_eq pp expected actual =
  if expected <> actual then
    failwith (Printf.sprintf "Expected %s but got %s" (pp expected) (pp actual))

let assert_true msg b =
  if not b then failwith msg

let assert_ok = function
  | Ok v -> v
  | Error _ -> failwith "Expected Ok but got Error"

let assert_error = function
  | Ok _ -> failwith "Expected Error but got Ok"
  | Error e -> e

(** ============================================================================
    Symbol Tests
    ============================================================================ *)

let test_symbol () =
  print_endline "\n=== Symbol Tests ===";
  
  test "valid symbol" (fun () ->
    let sym = Message_types.Symbol.of_string "IBM" in
    assert_true "should be Some" (Option.is_some sym);
    assert_eq Fun.id "IBM" (Message_types.Symbol.to_string (Option.get sym))
  );
  
  test "max length symbol (8 chars)" (fun () ->
    let sym = Message_types.Symbol.of_string "ABCDEFGH" in
    assert_true "should be Some" (Option.is_some sym)
  );
  
  test "too long symbol rejected" (fun () ->
    let sym = Message_types.Symbol.of_string "ABCDEFGHI" in
    assert_true "should be None" (Option.is_none sym)
  );
  
  test "empty symbol rejected" (fun () ->
    let sym = Message_types.Symbol.of_string "" in
    assert_true "should be None" (Option.is_none sym)
  );
  
  test "symbol to bytes (8-byte padded)" (fun () ->
    let sym = Message_types.Symbol.of_string_exn "IBM" in
    let bytes = Message_types.Symbol.to_bytes sym in
    assert_eq string_of_int 8 (Bytes.length bytes);
    assert_eq Fun.id "IBM" (Bytes.sub_string bytes 0 3);
    assert_eq string_of_int 0 (Bytes.get_uint8 bytes 3)
  )

(** ============================================================================
    CSV Codec Tests
    ============================================================================ *)

let test_csv_codec () =
  print_endline "\n=== CSV Codec Tests ===";
  
  test "parse new order" (fun () ->
    let line = "N, 1, IBM, 10000, 50, B, 42" in
    let msg = assert_ok (Csv_codec.parse_input_msg line) in
    match msg with
    | Message_types.NewOrder o ->
      assert_eq Int32.to_string 1l o.user_id;
      assert_eq Fun.id "IBM" (Message_types.Symbol.to_string o.symbol);
      assert_eq Int32.to_string 10000l o.price;
      assert_eq Int32.to_string 50l o.quantity;
      assert_true "side should be Buy" (o.side = Message_types.Buy);
      assert_eq Int32.to_string 42l o.user_order_id
    | _ -> failwith "Expected NewOrder"
  );
  
  test "parse sell order" (fun () ->
    let line = "N, 2, AAPL, 15000, 100, S, 1" in
    let msg = assert_ok (Csv_codec.parse_input_msg line) in
    match msg with
    | Message_types.NewOrder o ->
      assert_true "side should be Sell" (o.side = Message_types.Sell)
    | _ -> failwith "Expected NewOrder"
  );
  
  test "parse cancel" (fun () ->
    let line = "C, 1, 42" in
    let msg = assert_ok (Csv_codec.parse_input_msg line) in
    match msg with
    | Message_types.CancelOrder c ->
      assert_eq Int32.to_string 1l c.cancel_user_id;
      assert_eq Int32.to_string 42l c.cancel_user_order_id
    | _ -> failwith "Expected CancelOrder"
  );
  
  test "parse flush" (fun () ->
    let line = "F" in
    let msg = assert_ok (Csv_codec.parse_input_msg line) in
    match msg with
    | Message_types.Flush -> ()
    | _ -> failwith "Expected Flush"
  );
  
  test "parse ack output" (fun () ->
    let line = "A, IBM, 1, 42" in
    let msg = assert_ok (Csv_codec.parse_output_msg line) in
    match msg with
    | Message_types.Ack a ->
      assert_eq Fun.id "IBM" (Message_types.Symbol.to_string a.ack_symbol);
      assert_eq Int32.to_string 1l a.ack_user_id;
      assert_eq Int32.to_string 42l a.ack_user_order_id
    | _ -> failwith "Expected Ack"
  );
  
  test "parse trade output" (fun () ->
    let line = "T, IBM, 1, 1, 2, 1, 10000, 50" in
    let msg = assert_ok (Csv_codec.parse_output_msg line) in
    match msg with
    | Message_types.Trade t ->
      assert_eq Fun.id "IBM" (Message_types.Symbol.to_string t.trade_symbol);
      assert_eq Int32.to_string 1l t.trade_user_id_buy;
      assert_eq Int32.to_string 10000l t.trade_price;
      assert_eq Int32.to_string 50l t.trade_quantity
    | _ -> failwith "Expected Trade"
  );
  
  test "parse TOB output" (fun () ->
    let line = "B, IBM, B, 10000, 50" in
    let msg = assert_ok (Csv_codec.parse_output_msg line) in
    match msg with
    | Message_types.TopOfBook tob ->
      assert_eq Fun.id "IBM" (Message_types.Symbol.to_string tob.tob_symbol);
      assert_true "side should be Buy" (tob.tob_side = Message_types.Buy);
      assert_eq Int32.to_string 10000l tob.tob_price;
      assert_eq Int32.to_string 50l tob.tob_quantity
    | _ -> failwith "Expected TopOfBook"
  );
  
  test "parse eliminated TOB" (fun () ->
    let line = "B, IBM, S, -, -" in
    let msg = assert_ok (Csv_codec.parse_output_msg line) in
    match msg with
    | Message_types.TopOfBook tob ->
      assert_eq Int32.to_string 0l tob.tob_price;
      assert_eq Int32.to_string 0l tob.tob_quantity;
      assert_true "should be eliminated" (Message_types.tob_is_eliminated tob)
    | _ -> failwith "Expected TopOfBook"
  );
  
  test "invalid message type" (fun () ->
    let line = "X, invalid" in
    let _ = assert_error (Csv_codec.parse_input_msg line) in
    ()
  )

(** ============================================================================
    Binary Codec Tests
    ============================================================================ *)

let test_binary_codec () =
  print_endline "\n=== Binary Codec Tests ===";
  
  test "magic byte is 0x4D" (fun () ->
    assert_eq string_of_int 0x4D Binary_codec.Wire.magic
  );
  
  test "encode new order has magic" (fun () ->
    let sym = Message_types.Symbol.of_string_exn "IBM" in
    let order : Message_types.new_order = {
      user_id = 1l;
      user_order_id = 42l;
      price = 10000l;
      quantity = 50l;
      side = Message_types.Buy;
      symbol = sym;
    } in
    let bytes = Binary_codec.encode_new_order order in
    assert_eq string_of_int 0x4D (Bytes.get_uint8 bytes 0);
    assert_eq string_of_int 0x4E (Bytes.get_uint8 bytes 1)
  );
  
  test "encode flush" (fun () ->
    let bytes = Binary_codec.encode_flush () in
    assert_eq string_of_int 2 (Bytes.length bytes);
    assert_eq string_of_int 0x4D (Bytes.get_uint8 bytes 0);
    assert_eq string_of_int 0x46 (Bytes.get_uint8 bytes 1)
  );
  
  test "big-endian encoding" (fun () ->
    let sym = Message_types.Symbol.of_string_exn "IBM" in
    let order : Message_types.new_order = {
      user_id = 0x01020304l;
      user_order_id = 1l;
      price = 10000l;
      quantity = 50l;
      side = Message_types.Buy;
      symbol = sym;
    } in
    let bytes = Binary_codec.encode_new_order order in
    assert_eq string_of_int 0x01 (Bytes.get_uint8 bytes 2);
    assert_eq string_of_int 0x02 (Bytes.get_uint8 bytes 3);
    assert_eq string_of_int 0x03 (Bytes.get_uint8 bytes 4);
    assert_eq string_of_int 0x04 (Bytes.get_uint8 bytes 5)
  )

(** ============================================================================
    Protocol Detection Tests
    ============================================================================ *)

let test_protocol_detection () =
  print_endline "\n=== Protocol Detection Tests ===";
  
  test "detect binary (magic byte)" (fun () ->
    let buf = Bytes.of_string "\x4D\x41IBM..." in
    let detected = Binary_codec.detect_protocol buf (Bytes.length buf) in
    assert_true "should detect Binary" (detected = Binary_codec.Detected_Binary)
  );
  
  test "detect CSV (letter + comma)" (fun () ->
    let buf = Bytes.of_string "A, IBM, 1, 1" in
    let detected = Binary_codec.detect_protocol buf (Bytes.length buf) in
    assert_true "should detect CSV" (detected = Binary_codec.Detected_CSV)
  );
  
  test "detect FIX" (fun () ->
    let buf = Bytes.of_string "8=FIX.4.4|9=..." in
    let detected = Binary_codec.detect_protocol buf (Bytes.length buf) in
    assert_true "should detect FIX" (detected = Binary_codec.Detected_FIX)
  )

(** ============================================================================
    Message Types Tests
    ============================================================================ *)

let test_message_types () =
  print_endline "\n=== Message Types Tests ===";
  
  test "side to char" (fun () ->
    assert_eq (String.make 1) 'B' (Message_types.side_to_char Message_types.Buy);
    assert_eq (String.make 1) 'S' (Message_types.side_to_char Message_types.Sell)
  );
  
  test "side of char" (fun () ->
    assert_true "B -> Buy" (Message_types.side_of_char 'B' = Some Message_types.Buy);
    assert_true "S -> Sell" (Message_types.side_of_char 'S' = Some Message_types.Sell);
    assert_true "X -> None" (Message_types.side_of_char 'X' = None)
  );
  
  test "side to uint8 (ASCII values)" (fun () ->
    assert_eq string_of_int 0x42 (Message_types.side_to_uint8 Message_types.Buy);
    assert_eq string_of_int 0x53 (Message_types.side_to_uint8 Message_types.Sell)
  );
  
  test "tob_is_eliminated" (fun () ->
    let sym = Message_types.Symbol.of_string_exn "IBM" in
    let eliminated : Message_types.top_of_book = { 
      tob_symbol = sym; 
      tob_side = Message_types.Buy; 
      tob_price = 0l; 
      tob_quantity = 0l 
    } in
    let active : Message_types.top_of_book = { 
      tob_symbol = sym; 
      tob_side = Message_types.Buy; 
      tob_price = 100l; 
      tob_quantity = 50l 
    } in
    assert_true "should be eliminated" (Message_types.tob_is_eliminated eliminated);
    assert_true "should not be eliminated" (not (Message_types.tob_is_eliminated active))
  )

(** ============================================================================
    Main
    ============================================================================ *)

let () =
  print_endline "========================================";
  print_endline "  Matching Engine Client Tests";
  print_endline "========================================";
  
  test_symbol ();
  test_csv_codec ();
  test_binary_codec ();
  test_protocol_detection ();
  test_message_types ();
  
  print_endline "\n========================================";
  Printf.printf "  Results: %d/%d passed" !pass_count !test_count;
  if !fail_count > 0 then
    Printf.printf " (%d FAILED)" !fail_count;
  print_endline "\n========================================";
  
  if !fail_count > 0 then exit 1
