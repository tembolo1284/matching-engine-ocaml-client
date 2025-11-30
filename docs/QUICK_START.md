# Quick Start Guide

Get the OCaml matching engine client up and running in 5 minutes.

## Prerequisites

- OCaml 4.14.0 or later
- Dune 3.0 or later
- A running matching engine server (your C implementation)

### Installing OCaml (if needed)

**macOS:**
```bash
brew install opam
opam init
opam switch create 4.14.0
eval $(opam env)
```

**Ubuntu/Debian:**
```bash
sudo apt install opam
opam init
opam switch create 4.14.0
eval $(opam env)
```

## Building the Client
```bash
# Clone or copy the project
cd matching-engine-ocaml-client

# Build everything
dune build

# Run tests to verify
dune test
```

Expected test output:
```
========================================
  Matching Engine Client Tests
========================================

=== Symbol Tests ===
  valid symbol... OK
  max length symbol (8 chars)... OK
  ...

=== CSV Codec Tests ===
  parse new order... OK
  parse sell order... OK
  ...

========================================
  Results: 25/25 passed
========================================
```

## Starting the Server

In a separate terminal, start your C matching engine:
```bash
# TCP mode (recommended for getting started)
./matching_engine --tcp --port 1234

# Or with multicast market data
./matching_engine --tcp --port 1234 --multicast 239.255.0.1:5000
```

## Running the Client

### Interactive Mode (Default)
```bash
dune exec me_client -- -h localhost -p 1234
```

You'll see:
```
Matching Engine Client
Connected to: tcp://localhost:1234 (CSV)
Type 'help' for commands, 'quit' to exit.

> 
```

Try some commands:
```
> buy IBM 10000 50
[ACK] IBM user=1 order=1
[TOB] IBM B: 50 @ 10000

> sell IBM 10000 50
[ACK] IBM user=1 order=2
[TRADE] IBM: BUY 1/1 <-> SELL 1/2 @ 10000 x 50
[TOB] IBM B: EMPTY
[TOB] IBM S: EMPTY

> flush
Flushed 0 orders

> quit
Goodbye!
```

### Command Reference

| Command | Description | Example |
|---------|-------------|---------|
| `buy SYMBOL PRICE QTY` | Place buy order | `buy IBM 10000 50` |
| `sell SYMBOL PRICE QTY` | Place sell order | `sell AAPL 15000 100` |
| `cancel ORDER_ID` | Cancel order | `cancel 1` |
| `flush` | Cancel all orders | `flush` |
| `status` | Show connection info | `status` |
| `help` | Show help | `help` |
| `quit` | Exit | `quit` |

### Run a Test Scenario
```bash
# List available scenarios
dune exec me_client -- --list-scenarios

# Run scenario 2 (simple trade)
dune exec me_client -- -h localhost -p 1234 -s 2
```

Output:
```
Running scenario 2...
Scenario 2: Simple Trade
========================================
Status: PASS
Commands sent: 3
Responses received: 6
Trades executed: 1
Elapsed: 5.23 ms
```

### Load Testing (Fire-and-Forget)
```bash
# Send 10,000 orders as fast as possible
dune exec me_client -- -h localhost -p 1234 -l 10000 --symbol IBM
```

Output:
```
Sending 10000 orders (fire-and-forget)...
Fire-and-Forget Results
=======================
Messages sent: 10000
Send errors: 0
Elapsed: 0.234 sec
Throughput: 42735 msg/sec
```

### Subscribe to Multicast Market Data
```bash
# In terminal 1: Start server with multicast
./matching_engine --tcp --port 1234 --multicast 239.255.0.1:5000

# In terminal 2: Subscribe to multicast
dune exec me_client -- -m 239.255.0.1:5000

# In terminal 3: Send orders (generates market data)
dune exec me_client -- -h localhost -p 1234
> buy IBM 10000 50
```

Terminal 2 will show:
```
Subscribed! Waiting for market data (Ctrl+C to stop)...

A, IBM, 1, 1
B, IBM, B, 10000, 50
^C

--- Multicast Statistics ---
Packets received: 2
Messages decoded: 2
Decode errors: 0
Bytes received: 48
Elapsed: 5.234s
Throughput: 0.38 msg/sec
Protocol: CSV
```

## Auto-Discovery

The client automatically detects:
1. **Transport**: TCP vs UDP
2. **Protocol**: CSV vs Binary vs FIX
```bash
# Let the client figure it out
dune exec me_client -- -h localhost -p 1234

# Or discover and show results
dune exec me_client -- -h localhost -p 1234 --discover
```

Output:
```
Discovering server at localhost:1234...
Discovered:
  Transport: TCP
  Protocol: CSV
  Host: localhost
  Port: 1234
```

## Force Specific Transport/Protocol
```bash
# Force TCP + Binary
dune exec me_client -- -h localhost -p 1234 --tcp --binary

# Force UDP + CSV
dune exec me_client -- -h localhost -p 1234 --udp --csv
```

## Run Commands from a File

Create a file `commands.txt`:
```
N, 1, IBM, 10000, 50, B, 1
N, 1, IBM, 10000, 50, S, 2
F
```

Run it:
```bash
dune exec me_client -- -h localhost -p 1234 -f commands.txt
```

## Verbose Mode

Add `-v` for detailed output:
```bash
dune exec me_client -- -h localhost -p 1234 -v
```

Shows connection details and all sent/received messages.

## Common Issues

### "Connection failed: Connection refused"

The server isn't running or wrong port:
```bash
# Check server is running
ps aux | grep matching_engine

# Try the correct port
dune exec me_client -- -h localhost -p 12345
```

### "Discovery failed: Timeout"

Server might be UDP-only and not responding to probe:
```bash
# Force UDP mode
dune exec me_client -- -h localhost -p 1234 --udp
```

### "Invalid symbol"

Symbols must be 1-8 characters:
```bash
# Good
> buy IBM 100 50

# Bad (too long)
> buy VERYLONGSYMBOL 100 50
```

### Multicast not receiving data

Check multicast is enabled on your network:
```bash
# Test loopback first
./matching_engine --tcp --port 1234 --multicast 239.255.0.1:5000
dune exec me_client -- -m 239.255.0.1:5000
```

## Next Steps

- Read [ARCHITECTURE.md](ARCHITECTURE.md) for design details
- Read [TESTING.md](TESTING.md) for comprehensive testing guide
- Check the source code in `lib/` for API documentation

## CLI Reference
```
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
    --help                  Show help
```
