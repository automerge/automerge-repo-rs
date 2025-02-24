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
