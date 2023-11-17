# Automerge-repo-rs

Project goal: add an integration layer between [Automerge](https://github.com/automerge/automerge) and client code, compatible with any Rust async runtime.

## Examples 

### Multipe TCP clients, one server with hardcoded IP, in memory storage.

1. Start the server:
   - `cargo run --example tcp-example --features="tokio" -- --tcp-run-ip 127.0.0.1:2345 --http-run-ip 0.0.0.0:3001`
2. Start any number of clients:
   - `cargo run --example tcp-example --features="tokio" -- --other-ip 127.0.0.1:2345 --http-run-ip 0.0.0.0:3002`
3. Create a new document at the server:
   - `curl 0.0.0.0:3001/new_doc`
   - The document id is returned as json.
4. Request the document:
   - `curl --json '{document-id}' 0.0.0.0:3002/request_doc`(document id argument should look like`'"f9b1a2c6-ca05-4fc7-a02f-7e3d83f1bb49"'`)
5. Presss ctr-c at every terminal tab.
6. A successful run will print out the expected synced documents. 

### Distributed Bakery Algorithm

An implementation of the [distributed bakery algorithm](https://lamport.azurewebsites.net/pubs/bakery/dbakery-complete.pdf), 
based on this [TLA+ specification](https://gist.github.com/gterzian/d9898f3206fedb921d916399d287780f).

1. Start the bootstap peer:
   - `cargo run --release --example distributed-bakery --features="tokio" -- --bootstrap --customer-id "1"`
2. Start two other peers:
   - `cargo run --release --example distributed-bakery --features="tokio" -- --customer-id "2"`
   - `cargo run --release --example distributed-bakery --features="tokio" -- --customer-id "3"`
3. Watch the peers request new increments from each other, and enter/exit their critical sections(printed out).
