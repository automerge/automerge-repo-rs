# Spanreed

Project goal: add an integration layer between automerge and client code, compatible with any async runtime.

## Examples 

### Multipe TCP clients, one server with hardcoded IP, in memory storage.

1. Start the server in two separate terminals:
   - `cargo run --example tcp-example -- --run-ip 127.0.0.1:2345`
2. Start any number of clients
   - `cargo run --example tcp-example -- --other-ip 127.0.0.1:2345`
3. Press ctr-c. 
4. A successful run prints out a number of synced documents. 