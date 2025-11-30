# Matching Engine OCaml Client

A high-performance OCaml client for interacting with the C matching engine. Supports auto-discovery of transport and protocol, interactive trading, batch testing, and multicast market data subscription.

## Features

- **Auto-Discovery**: Automatically detects TCP/UDP transport and CSV/Binary/FIX protocol
- **Interactive Mode**: REPL with colored output for manual trading
- **Batch Mode**: Run predefined scenarios or commands from files
- **Fire-and-Forget**: High-throughput load testing without waiting for responses
- **Multicast Subscriber**: Subscribe to market data feeds
- **Cross-Platform**: Works on macOS and Linux

## Requirements

- OCaml 4.14.0+
- Dune 3.0+
- Unix (standard library)

No external dependencies beyond the OCaml standard library.

## Installation

### From Source
```bash
git clone https://github.com/yourusername/matching-engine-ocaml-client.git
cd matching-engine-ocaml-client

# Build
dune build

# Run tests
dune test

# Install globally (optional)
dune install
```

### Quick Check
```bash
# Show help
dune exec me_client -- --help

# List available test scenarios
dune exec me_client -- --list-scenarios
```

## Quick Start

### 1. Start the Server
```bash
# Your C matching engine
./matching_engine --tcp --port 1234
```

### 2. Connect with the Client
```bash
dune exec me_client -- -h localhost -p 1234
```

### 3. Trade!
```
> buy IBM 10000 50
[ACK] IBM user=1 order=1
[TOB] IBM B: 50 @ 10000

> sell IBM 10000 50
[TRADE] IBM: BUY 1/1 <-> SELL 1/2 @ 10000 x 50

> quit
```

See [docs/QUICK_START.md](docs/QUICK_START.md) for detailed instructions.

## Usage

### Interactive Mode (Default)
```bash
me_client -h localhost -p 1234
```

Commands:
| Command | Description |
|---------|-------------|
| `buy SYMBOL PRICE QTY` | Place buy order |
| `sell SYMBOL PRICE QTY` | Place sell order |
| `cancel ORDER_ID` | Cancel order |
| `flush` | Cancel all orders |
| `status` | Connection info |
| `help` | Show help |
| `quit` | Exit |

### Run Test Scenario
```bash
# Run scenario 2 (simple trade)
me_client -h localhost -p 1234 -s 2
```

Available scenarios:
1. Balanced book (no trades)
2. Simple trade
3. Partial fill
4. Price-time priority
5. Multiple symbols (dual-processor)
6. Cancel order

### Load Testing
```bash
# Fire-and-forget 10,000 orders
me_client -h localhost -p 1234 -l 10000 --symbol IBM
```

### Execute Commands from File
```bash
me_client -h localhost -p 1234 -f commands.txt
```

File format (CSV):
```
N, 1, IBM, 10000, 50, B, 1
N, 1, IBM, 10000, 50, S, 2
F
```

### Multicast Subscriber
```bash
me_client -m 239.255.0.1:5000
```

### Force Transport/Protocol
```bash
# Force TCP + Binary
me_client -h localhost -p 1234 --tcp --binary

# Force UDP + CSV  
me_client -h localhost -p 1234 --udp --csv
```

### Discovery Only
```bash
me_client -h localhost -p 1234 --discover
```

Output:
```
Discovered:
  Transport: TCP
  Protocol: CSV
  Host: localhost
  Port: 1234
```

## Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                         CLI (main.ml)                        │
│   Argument parsing, mode selection, signal handling          │
└─────────────────────────┬───────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│  Interactive  │ │    Batch      │ │   Multicast   │
│     REPL      │ │   Scenarios   │ │  Subscriber   │
└───────┬───────┘ └───────┬───────┘ └───────┬───────┘
        │                 │                 │
        └─────────────────┼─────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    Engine Client                             │
│   High-level API: connect, send_order, recv, etc.           │
└─────────────────────────┬───────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│   Transport   │ │   Discovery   │ │   Multicast   │
│   TCP/UDP     │ │  Auto-detect  │ │     UDP       │
└───────┬───────┘ └───────────────┘ └───────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                    Protocol Codecs                           │
│   ┌─────────────┐ ┌─────────────┐ ┌─────────────┐          │
│   │   Binary    │ │     CSV     │ │  (FIX TBD)  │          │
│   │  0x4D magic │ │  "N, ..."   │ │  "8=FIX..." │          │
│   └─────────────┘ └─────────────┘ └─────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

## Protocol Support

### CSV Protocol

Human-readable text format:
```
# Input
N, userId, symbol, price, qty, side, orderId
C, userId, orderId
F

# Output
A, symbol, userId, orderId
C, symbol, userId, orderId
T, symbol, buyUser, buyOid, sellUser, sellOid, price, qty
B, symbol, side, price, qty
```

### Binary Protocol

High-performance binary format:
- Magic byte: `0x4D` ('M')
- Network byte order (big-endian)
- 8-byte null-padded symbols
- Message types: 'N', 'C', 'F', 'A', 'X', 'T', 'B'

### FIX Protocol

Placeholder for future FIX 4.4 support. Detection implemented (`8=FIX...`), codec not yet written.

## Project Structure
```
matching-engine-ocaml-client/
├── dune-project                    # Project metadata & opam config
├── dune                            # Root build config (optional)
├── README.md
│
├── bin/
│   ├── dune                        # Executable build config
│   └── main.ml                     # CLI entry point
│
├── lib/
│   ├── dune                        # Library build config
│   │
│   ├── protocol/
│   │   ├── message_types.ml        # Core types, Symbol module
│   │   ├── binary_codec.ml         # Binary wire protocol (0x4D magic)
│   │   └── csv_codec.ml            # CSV parsing/encoding
│   │
│   ├── network/
│   │   ├── discovery.ml            # Auto-detect TCP/UDP + protocol
│   │   ├── transport.ml            # Unified TCP/UDP abstraction
│   │   └── multicast.ml            # Multicast subscriber
│   │
│   └── client/
│       ├── engine_client.ml        # High-level client API
│       ├── interactive.ml          # REPL mode with colors
│       └── batch.ml                # Scenarios + fire-and-forget
│
├── test/
│   ├── dune                        # Test build config
│   └── test_protocol.ml            # Unit tests
│
└── docs/
    ├── QUICK_START.md              # Getting started guide
    ├── ARCHITECTURE.md             # Design documentation
    └── TESTING.md                  # Testing guide
```

## Module Organization

### Protocol Layer (`lib/protocol/`)

| Module | Description |
|--------|-------------|
| `Message_types` | Core types: `side`, `Symbol.t`, `input_msg`, `output_msg` |
| `Binary_codec` | Binary protocol encoding/decoding with 0x4D magic byte |
| `Csv_codec` | CSV protocol parsing and formatting |

### Network Layer (`lib/network/`)

| Module | Description |
|--------|-------------|
| `Discovery` | Auto-detect transport (TCP/UDP) and protocol (CSV/Binary/FIX) |
| `Transport` | Unified send/receive abstraction with TCP framing |
| `Multicast` | UDP multicast group subscription for market data |

### Client Layer (`lib/client/`)

| Module | Description |
|--------|-------------|
| `Engine_client` | High-level trading API: connect, buy, sell, cancel |
| `Interactive` | REPL implementation with colored output |
| `Batch` | Predefined scenarios and fire-and-forget load testing |

## Library API

Use the client library in your own OCaml code:
```ocaml
open Matching_engine_client

(* Create and connect *)
let config = Engine_client.default_config ~host:"localhost" ~port:1234 () in
let client = Engine_client.create config in

match Engine_client.connect client with
| Error e -> 
    Printf.printf "Failed: %s\n" (Engine_client.client_error_to_string e)
| Ok () ->
    (* Place an order *)
    let symbol = Message_types.Symbol.of_string_exn "IBM" in
    begin match Engine_client.buy client ~symbol ~price:10000l ~quantity:50l () with
    | Ok (order_id, responses) ->
        Printf.printf "Order %ld placed\n" order_id;
        List.iter (fun msg ->
          Printf.printf "%s\n" (Message_types.output_msg_to_csv msg)
        ) responses
    | Error e ->
        Printf.printf "Error: %s\n" (Engine_client.client_error_to_string e)
    end;
    
    (* Disconnect *)
    Engine_client.disconnect client
```

## Testing
```bash
# Run all tests
dune test

# Run with verbose output
dune test --force

# Test against live server
dune exec me_client -- -h localhost -p 1234 -s 2
```

See [docs/TESTING.md](docs/TESTING.md) for comprehensive testing guide.

## Performance

Tested on Apple M1:

| Mode | Throughput |
|------|------------|
| Fire-and-forget (TCP) | ~40,000 msg/sec |
| Fire-and-forget (UDP) | ~80,000 msg/sec |
| Interactive | N/A (human-limited) |

Note: Actual throughput depends on server, network, and message size.

## Comparison with C Client

| Feature | C Client | OCaml Client |
|---------|----------|--------------|
| Interactive mode | ✓ | ✓ |
| Scenarios | ✓ | ✓ |
| Binary protocol | ✓ | ✓ |
| CSV protocol | ✓ | ✓ |
| Auto-discovery | ✗ | ✓ |
| Multicast subscriber | Separate tool | Built-in |
| Fire-and-forget | ✗ | ✓ |
| Colored output | ✗ | ✓ |
| Type safety | Runtime | Compile-time |

## Design Philosophy

This client follows several principles inspired by safety-critical coding:

1. **Exhaustive Pattern Matching**: All message types handled explicitly
2. **Result Types**: Errors are values, not exceptions
3. **Tail Recursion**: Loops that won't blow the stack
4. **Minimal Dependencies**: Only Unix stdlib
5. **Compile-Time Safety**: OCaml's type system catches bugs early

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed design documentation.


## Related Projects

- [Matching Engine (C)](../matching-engine-c) - The server implementation
- [Zig Client](../matching-engine-zig-client) - Zig client implementation

