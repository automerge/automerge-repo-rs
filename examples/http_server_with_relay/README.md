# Http relay

This example consists of two node types:

1. A relay server, which just relays sync messages 
2. Peers which manage a repository and expose an http API to the documents
   they manage and which only communicate by sending messages to the relay 
   server.

## Usage

Run a relay server

```bash
cargo run --example http_server_with_relay run-peer --ip 0.0.0.0:3001 --relay-ip 0.0.0.0:3000
```

Now run two repo peers pointing at the relay server (obviously run each of the following commands in a separate terminal)

```bash
cargo run --example http_server_with_relay run-peer --ip 0.0.0.0:3001 --relay-ip 0.0.0.0:3000
cargo run --example http_server_with_relay run-peer --ip 0.0.0.0:3002 --relay-ip 0.0.0.0:3000
```

Now, create a document on one peer

```bash
DOC_ID=$(curl 0.0.0.0:3001/new_doc)
curl --json $DOC_ID 0.0.0.0:3001/edit_doc/somekey
curl --json $DOC_ID 0.0.0.0:3001/print_doc
```

This should print `{"key": "somekey"}` to the console where you `curl`ed

Finally, load the document on the other peer

```bash
curl --json $DOC_ID 0.0.0.0:3002/load_doc
curl --json $DOC_ID 0.0.0.0:3002/print_doc
```
