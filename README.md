# Spanreed

Project goal: add an integration layer between automerge and client code, compatible with any async runtime.

## Examples 

### HTTP servers, with one relay server

1. Start the relay server(run-ip equals relay-ip): `cargo run --example http-server-with-relay  -- --run-ip 0.0.0.0:3001 --relay-ip 0.0.0.0:3001`
2. Start any number of peers: 
   - `cargo run --example http-server-with-relay  -- --run-ip 0.0.0.0:3000 --relay-ip 0.0.0.0:3001`
   - `cargo run --example http-server-with-relay  -- --run-ip 0.0.0.0:3002 --relay-ip 0.0.0.0:3001`
   - `cargo run --example http-server-with-relay  -- --run-ip 0.0.0.0:3003 --relay-ip 0.0.0.0:3001`
3. Create a document: `curl 0.0.0.0:3000/new_doc`. This returns a document id in the format `[["d3206efc-cff0-4cd5-ad16-9ecfee76a03f",1],1]`
4. Edit the document at the creator node: `curl --json '{document id}' 0.0.0.0:3000/edit_doc/test001`
5. Load the document at one, or several, peers(s):
   - `curl --json '{document id}' 0.0.0.0:3002/load_doc`
   - `curl --json '{document id}' 0.0.0.0:3003/load_doc`
   - Syncing servers should be printing out "Synced DocumentId" type of output.
6. Check the document state at at one, or several, peers(s):
   - `curl --json '{document id}' 0.0.0.0:3000/print_doc`
7. Make additional edits, at any peer, and repeat step 6. 


### TCP peers, with hardcoded IPs.

1. Start the two peers in two separate terminals:
   - `cargo run --example tcp-example -- --run-ip 127.0.0.1:2345 --other-ip 127.0.0.1:2346`
   - `cargo run --example tcp-example -- --run-ip 127.0.0.1:2346 --other-ip 127.0.0.1:2345`
2. Wait until no messages are printed anymore.
3. Press ctr-c. 
4. A successful run prints out "Stopped", failure panics. 