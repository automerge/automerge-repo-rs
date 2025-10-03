# 0.3.0 - 2025-10-03

### Breaking Changes

* Updated to `automerge@0.7.0`
* `PeerState` is now called `PeerDocState`
* `Repo::peer_state` is now called `Repo::peer_doc_state`
* `Repo::new_remote_repo`, `Repo::connect_stream`, `Repo::connect_tokio_Io`,
  `Repo::connect_tungstenite` and `Repo::accept_axum` now return a future which
  resolves to a `ConnFinishedReason` which indicates why the connection was
  terminated instead of a `Result<(), NetworkError>`

### Added

* `Repo::peer_conn_info` which returns the connection state of all connected
  peers (when we last sent and received a message and what documents they are
  syncing with)
* `Repo::peer_conn_info_changes` which is a stream of changes to the connection
  state of all connected peers.

### Fixed

* Fixed an error where sinks which threw an error could be polled after the error
  was thrown
* Fixed an error where sync messages could be dropped if a document was loading
  whilst they were received (would manifest as stuck sync).

## 0.2.2 - 2025-02-24

### Fixed

* Fix an issue where loading a document whilst it is concurrently being synced
by another peer would cause the document to resolve as empty (`e8c4ba79`)

## 0.2.1 - 2025-02-21

### Added

* Updated to `automerge@0.6.0`

### Fixed

* Fix an issue with the FsStorage implementation which could cause
  data loss during compaction. (`b3df2f43`)

## 0.2.0 - 2024-11-28

### Added

* Add `Repo::peer_state` which returns information about how in-sync we are
  with another peer. (`469a3556`)

### Fixed

* Fix an issue where the Repo could fail to respond to sync messages for a
  document which is authorized. (`a5b19f79`)
