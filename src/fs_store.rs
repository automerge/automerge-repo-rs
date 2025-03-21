use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};

use crate::DocumentId;
use automerge::ChangeHash;
pub use error::Error;
use error::ErrorKind;

/// A database that stores documents in the filesystem
///
/// This store is designed so that concurrent processes can use the same
/// data directory without coordination.
///
/// > Note that this does not implement [`crate::Storage`] directly but rather is
/// > intended to be wrapped in async runtime specific implementations.
///
/// ## Storage layout
///
/// In order to reduce the number of files in a single directory we follow git
/// in splaying the files over 256 subdirectories using the first two bytes of
/// the SHA256 hash of the document ID. Then within each subdirectory we use
/// the hex encoding of the document ID as a directory within which we store the
/// incremental and snapshots saves of a document. I.e.
///
/// ```sh
/// <root>/
///  <first two bytes of SHA256 hash of document ID>/
///    <hex encoded bytes of document ID>/
///      <incremental save 1>.incremental
///      <incremental save 2>.incremental
///      <sha256(heads)>.snapshot
/// ```
///
/// We use the SHA256 rather than the document ID directly because whilst we
/// use a UUID to generate document IDs, we don't know how they are generated
/// on other peers and for the splaying to be useful we need to guarantee a
/// uniform distribution of documents across the subdirectories.
///
/// ## Compaction
///
/// In order to support compaction we do the following:
///
/// 1. Load all the incremental and snapshot files we are aware of from the
///    filesystem.
/// 2. Load the data into an automerge document
/// 3. `automerge::Automerge::save` the document to a temporary file
/// 4. Rename the temporary file to a file in the data directory named
///    `<sha256(heads)>.snapshot`
/// 5. Delete all the files we loaded in step 1.
///
/// The fact that we name the file after the heads of the document means that
/// any two processes which load the same sets of changes will produce the same
/// filename whilst processes with different sets of changes will produce
/// different filenames. This means that we can safely delete the files we
/// loaded because even if another process is running the same compaction step
/// concurrently it doesn't matter because if they end up loading a different
/// set of changes to us then they'll produce a different snapshot filename
/// and the next load will load both of them so no data is lost.
///
/// Renames are atomic so if the compaction process crashes then we don't get a
/// half finished snapshot file. Deleting the inputs after the rename means that
/// the worst case is that we have some leftover incremental files which will
/// be deleted on the next compaction.
#[derive(Debug)]
pub struct FsStore {
    root: std::path::PathBuf,
    tmpdir: std::path::PathBuf,
}

impl FsStore {
    /// Creates a new [`FsStore`] from a [`Path`].
    ///
    /// This will attempt to create the root directory and throw an error if
    /// it does not exist.
    pub fn open<P: AsRef<Path>>(root: P) -> Result<Self, std::io::Error> {
        let root = root.as_ref();
        std::fs::create_dir_all(root)?;
        Ok(Self {
            root: root.into(),
            tmpdir: root.into(),
        })
    }

    /// Overrides the tmpdir directory used for temporary files.
    ///
    /// The default is to use the root directory passed to [`FsStore::open`].
    ///
    /// The tmpdir used must be on the same mount point as the root directory,
    /// otherwise the store will throw an error on writing data.
    ///
    /// # Errors
    ///
    /// This will attempt to create the tmpdir directory and throw an error if
    /// it does not exist.
    pub fn with_tmpdir<P: AsRef<Path>>(self, tmpdir: P) -> Result<Self, std::io::Error> {
        let tmpdir = tmpdir.as_ref();
        std::fs::create_dir_all(tmpdir)?;
        Ok(Self {
            tmpdir: tmpdir.into(),
            ..self
        })
    }

    pub fn get(&self, id: &DocumentId) -> Result<Option<Vec<u8>>, Error> {
        let chunks = Chunks::load(&self.root, id)?;
        let Some(chunks) = chunks else {
            return Ok(None);
        };
        let mut result = Vec::new();
        result.extend(chunks.snapshots.into_values().flatten());
        result.extend(chunks.incrementals.into_values().flatten());
        Ok(Some(result))
    }

    pub fn list(&self) -> Result<Vec<DocumentId>, Error> {
        let mut result = HashSet::new();
        for entry in std::fs::read_dir(&self.root)
            .map_err(|e| Error(ErrorKind::ErrReadingRootPath(self.root.clone(), e)))?
        {
            let entry =
                entry.map_err(|e| Error(ErrorKind::ErrReadingRootPath(self.root.clone(), e)))?;
            if entry
                .metadata()
                .map_err(|e| Error(ErrorKind::ErrReadingLevel1Path(entry.path(), e)))?
                .is_file()
            {
                tracing::warn!(
                    non_dir_path=%entry.path().display(),
                    "unexpected non-directory at level1 of database"
                );
                continue;
            }
            let level1 = entry.path();
            let entries = level1
                .read_dir()
                .map_err(|e| Error(ErrorKind::ErrReadingLevel1Path(level1.clone(), e)))?;
            for entry in entries {
                let entry =
                    entry.map_err(|e| Error(ErrorKind::ErrReadingLevel1Path(level1.clone(), e)))?;
                let metadata = entry
                    .metadata()
                    .map_err(|e| Error(ErrorKind::ErrReadingLevel2Path(entry.path(), e)))?;
                if !metadata.is_dir() {
                    tracing::warn!(
                        non_dir_path=%entry.path().display(),
                        "unexpected non-directory at level2 of database"
                    );
                    continue;
                }
                let Some(doc_paths) = DocIdPaths::parse(entry.path()) else {
                    tracing::warn!(
                        non_doc_path=%entry.path().display(),
                        "unexpected non-document path at level2 of database"
                    );
                    continue;
                };
                result.insert(doc_paths.doc_id);
            }
        }
        Ok(result.into_iter().collect())
    }

    #[tracing::instrument(skip(self, changes))]
    pub fn append(&self, id: &DocumentId, changes: &[u8]) -> Result<(), Error> {
        tracing::debug!("writing incremental change");
        let paths = DocIdPaths::from(id);
        std::fs::create_dir_all(paths.level2_path(&self.root)).map_err(|e| {
            Error(ErrorKind::CreateLevel2Path(
                paths.level2_path(&self.root),
                e,
            ))
        })?;

        let chunk_name = SavedChunkName::new_incremental(changes);
        write_chunk(&self.root, &paths, changes, chunk_name, &self.tmpdir)?;

        Ok(())
    }

    #[tracing::instrument(skip(self, full_doc))]
    pub fn compact(&self, id: &DocumentId, full_doc: &[u8]) -> Result<(), Error> {
        tracing::debug!("compacting document");
        let paths = DocIdPaths::from(id);

        let mut doc = automerge::Automerge::load(full_doc)
            .map_err(|e| Error(ErrorKind::LoadDocToCompact(e)))?;

        // Load all the data we have into a doc
        match Chunks::load(&self.root, id) {
            Ok(Some(chunks)) => {
                chunks
                    .add_to_doc(&mut doc)
                    .map_err(|e| Error(ErrorKind::LoadDocToCompact(e)))?;

                // Write the snapshot
                let output_chunk_name = SavedChunkName::new_snapshot(doc.get_heads());
                let chunk = doc.save();
                write_chunk(
                    &self.root,
                    &paths,
                    &chunk,
                    output_chunk_name.clone(),
                    &self.tmpdir,
                )?;

                // Remove all the old data
                for incremental in chunks.incrementals.keys() {
                    let path = paths.chunk_path(&self.root, incremental);
                    std::fs::remove_file(&path)
                        .map_err(|e| Error(ErrorKind::DeleteChunk(path, e)))?;
                }
                let just_wrote = paths.chunk_path(&self.root, &output_chunk_name);
                for snapshot in chunks.snapshots.keys() {
                    let path = paths.chunk_path(&self.root, snapshot);

                    if path == just_wrote {
                        // This can happen if for some reason `compact` is called when the only thing
                        // on disk is a snapshot containing the changes we are being asked to compact
                        continue;
                    }

                    std::fs::remove_file(&path)
                        .map_err(|e| Error(ErrorKind::DeleteChunk(path, e)))?;
                }
            }
            Ok(None) => {
                std::fs::create_dir_all(paths.level2_path(&self.root)).map_err(|e| {
                    Error(ErrorKind::CreateLevel2Path(
                        paths.level2_path(&self.root),
                        e,
                    ))
                })?;

                // Write the snapshot
                let output_chunk_name = SavedChunkName::new_snapshot(doc.get_heads());
                write_chunk(
                    &self.root,
                    &paths,
                    full_doc,
                    output_chunk_name,
                    &self.tmpdir,
                )?;
            }
            Err(e) => {
                tracing::error!(doc_id=%id, %e, "error loading chunks");
            }
        }
        Ok(())
    }
}

fn write_chunk(
    root: &Path,
    paths: &DocIdPaths,
    chunk: &[u8],
    name: SavedChunkName,
    tmpdir: &Path,
) -> Result<(), Error> {
    // Write to a temp file and then rename to avoid partial writes
    let temp_dir =
        tempfile::TempDir::new_in(tmpdir).map_err(|e| Error(ErrorKind::CreateTempFile(e)))?;
    let temp_save_path = temp_dir.path().join(name.filename());
    let mut temp_save_file =
        File::create(&temp_save_path).map_err(|e| Error(ErrorKind::CreateTempFile(e)))?;
    temp_save_file
        .write_all(chunk)
        .map_err(|e| Error(ErrorKind::WriteTempFile(temp_save_path.clone(), e)))?;
    temp_save_file
        .sync_all()
        .map_err(|e| Error(ErrorKind::WriteTempFile(temp_save_path.clone(), e)))?;

    // Move the temporary file into a snapshot in the document data directory
    // with a name based on the hash of the heads of the document
    let output_path = paths.chunk_path(root, &name);
    tracing::trace!(?temp_save_path, ?output_path, "renaming chunk file");
    std::fs::rename(&temp_save_path, &output_path)
        .map_err(|e| Error(ErrorKind::RenameTempFile(temp_save_path, output_path, e)))?;

    Ok(())
}

struct DocIdPaths {
    doc_id: DocumentId,
    prefix: [u8; 2],
}

impl<'a> From<&'a DocumentId> for DocIdPaths {
    fn from(doc_id: &'a DocumentId) -> Self {
        let hash = ring::digest::digest(&ring::digest::SHA256, doc_id.as_ref())
            .as_ref()
            .to_vec();
        let mut prefix = [0u8; 2];
        prefix[0] = hash[0];
        prefix[1] = hash[1];
        Self {
            doc_id: doc_id.clone(),
            prefix,
        }
    }
}

impl DocIdPaths {
    fn parse<P: AsRef<Path>>(level2: P) -> Option<Self> {
        let level2 = level2.as_ref();
        let level1 = level2.parent()?.file_name()?.to_str()?;
        let prefix = hex::decode(level1).ok()?;
        let prefix = <[u8; 2]>::try_from(prefix).ok()?;

        let level2 = level2.file_name()?.to_str()?;
        let doc_id_bytes = hex::decode(level2).ok()?;
        let Ok(doc_id) = DocumentId::try_from(doc_id_bytes) else {
            tracing::error!(level2_path=%level2, "invalid document ID");
            return None;
        };
        let result = Self::from(&doc_id);
        if result.prefix != prefix {
            None
        } else {
            Some(result)
        }
    }

    /// The first level of the directory hierarchy, i.e.
    ///     `<root>/<first two bytes of SHA256 hash of document ID>`
    fn level1_path<P: AsRef<Path>>(&self, root: P) -> std::path::PathBuf {
        let mut path = root.as_ref().to_path_buf();
        path.push(hex::encode(self.prefix));
        path
    }

    /// The second level of the directory hierarchy, i.e.
    ///     `<root>/<first two bytes of SHA256 hash of document ID>/<hex encoded bytes of document ID>`
    fn level2_path<P: AsRef<Path>>(&self, root: P) -> std::path::PathBuf {
        let mut path = self.level1_path(root);
        path.push(hex::encode(self.doc_id.as_ref()));
        path
    }

    fn chunk_path(&self, root: &Path, chunk_name: &SavedChunkName) -> PathBuf {
        self.level2_path(root).join(chunk_name.filename())
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
enum ChunkType {
    Snapshot,
    Incremental,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
struct SavedChunkName {
    hash: Vec<u8>,
    chunk_type: ChunkType,
}

impl SavedChunkName {
    fn parse(name: &str) -> Option<Self> {
        let (name, chunk_type) = if let Some(name) = name.strip_suffix(".incremental") {
            (name, ChunkType::Incremental)
        } else if let Some(name) = name.strip_suffix(".snapshot") {
            (name, ChunkType::Snapshot)
        } else {
            return None;
        };
        let hash = hex::decode(name).ok()?;
        Some(Self { hash, chunk_type })
    }

    fn new_incremental(data: &[u8]) -> Self {
        Self {
            hash: hash_chunk(data),
            chunk_type: ChunkType::Incremental,
        }
    }

    fn new_snapshot(mut heads: Vec<ChangeHash>) -> Self {
        heads.sort();
        let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
        for head in heads {
            ctx.update(head.as_ref());
        }
        let hash = ctx.finish().as_ref().to_vec();
        Self {
            hash,
            chunk_type: ChunkType::Snapshot,
        }
    }

    fn filename(&self) -> String {
        let hash = hex::encode(&self.hash);
        match self.chunk_type {
            ChunkType::Incremental => format!("{}.incremental", hash),
            ChunkType::Snapshot => format!("{}.snapshot", hash),
        }
    }
}

fn hash_chunk(data: &[u8]) -> Vec<u8> {
    ring::digest::digest(&ring::digest::SHA256, data)
        .as_ref()
        .to_vec()
}

struct Chunks {
    snapshots: HashMap<SavedChunkName, Vec<u8>>,
    incrementals: HashMap<SavedChunkName, Vec<u8>>,
}

impl Chunks {
    fn load(root: &Path, doc_id: &DocumentId) -> Result<Option<Self>, Error> {
        let doc_id_hash = DocIdPaths::from(doc_id);
        let level2_path = doc_id_hash.level2_path(root);
        tracing::trace!(
            root=%root.display(),
            doc_id=?doc_id,
            doc_path=%level2_path.display(),
            "loading chunks from filesystem"
        );

        match level2_path.metadata() {
            Ok(m) => {
                if !m.is_dir() {
                    return Err(Error(ErrorKind::Level2PathNotDir(level2_path)));
                }
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    tracing::debug!(path=%level2_path.display(), "no level2 path found");
                    return Ok(None);
                }
                _ => return Err(Error(ErrorKind::ErrReadingLevel2Path(level2_path, e))),
            },
        };

        let mut snapshots = HashMap::new();
        let mut incrementals = HashMap::new();

        let entries = std::fs::read_dir(&level2_path)
            .map_err(|e| Error(ErrorKind::ErrReadingLevel2Path(level2_path.clone(), e)))?;
        for entry in entries {
            let entry = entry
                .map_err(|e| Error(ErrorKind::ErrReadingLevel2Path(level2_path.clone(), e)))?;
            let path = entry.path();
            if !path
                .metadata()
                .map_err(|e| Error(ErrorKind::ErrReadingChunkFileMetadata(path.clone(), e)))?
                .is_file()
            {
                tracing::warn!(bad_file=%path.display(), "unexpected non-file in level2 path");
                continue;
            }
            let Some(chunk_name) = entry.file_name().to_str().and_then(SavedChunkName::parse)
            else {
                tracing::warn!(bad_file=%path.display(), "unexpected non-chunk file in level2 path");
                continue;
            };
            tracing::trace!(chunk_path=%path.display(), "reading chunk file");
            let contents = match std::fs::read(&path) {
                Ok(c) => c,
                Err(e) => {
                    match e.kind() {
                        std::io::ErrorKind::NotFound => {
                            // Could be a concurrent process compacting, not an error
                            tracing::warn!(
                                missing_chunk_path=%path.display(),
                                "chunk file disappeared while reading chunks; ignoring",
                            );
                            continue;
                        }
                        _ => return Err(Error(ErrorKind::ErrReadingChunkFile(path, e))),
                    }
                }
            };
            match chunk_name.chunk_type {
                ChunkType::Incremental => {
                    incrementals.insert(chunk_name, contents);
                }
                ChunkType::Snapshot => {
                    snapshots.insert(chunk_name, contents);
                }
            }
        }
        Ok(Some(Chunks {
            snapshots,
            incrementals,
        }))
    }

    fn add_to_doc(&self, doc: &mut automerge::Automerge) -> Result<(), automerge::AutomergeError> {
        let mut bytes = Vec::new();
        for chunk in self.snapshots.values() {
            bytes.extend(chunk);
        }
        for chunk in self.incrementals.values() {
            bytes.extend(chunk);
        }
        doc.load_incremental(&bytes)?;
        Ok(())
    }
}

mod error {
    // This is a slightly verbose way of defining errors. The intention is to
    // have an opaque `Error` type which has a nice `Display` impl. We can't
    // use `thiserror` directly because that exposes the error enum. Therefore `Error` is just a
    // wrapper to keen an internal enum private.

    use std::path::PathBuf;

    pub struct Error(pub(super) ErrorKind);

    impl std::error::Error for Error {}

    impl std::fmt::Debug for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let message = self.0.to_string();
            f.debug_struct("Error").field("message", &message).finish()
        }
    }

    impl std::fmt::Display for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fmt(f)
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub(super) enum ErrorKind {
        #[error("level 2 path {0} is not a directory")]
        Level2PathNotDir(PathBuf),
        #[error("error reading root path {0}: {1}")]
        ErrReadingRootPath(PathBuf, std::io::Error),
        #[error("error reading level 1 path {0}: {1}")]
        ErrReadingLevel1Path(PathBuf, std::io::Error),
        #[error("error reading level 2 path {0}: {1}")]
        ErrReadingLevel2Path(PathBuf, std::io::Error),
        #[error("error reading chunk file metadata {0}: {1}")]
        ErrReadingChunkFileMetadata(PathBuf, std::io::Error),
        #[error("error reading chunk file {0}: {1}")]
        ErrReadingChunkFile(PathBuf, std::io::Error),
        #[error("error creating level 2 path {0}: {1}")]
        CreateLevel2Path(PathBuf, std::io::Error),
        #[error("error loading doc to compact: {0}")]
        LoadDocToCompact(automerge::AutomergeError),
        #[error("error creating temp file: {0}")]
        CreateTempFile(std::io::Error),
        #[error("error writing temp file {0}: {1}")]
        WriteTempFile(PathBuf, std::io::Error),
        #[error("error renaming temp file {0} to {1}: {2}")]
        RenameTempFile(PathBuf, PathBuf, std::io::Error),
        #[error("error deleting chunk {0}: {1}")]
        DeleteChunk(PathBuf, std::io::Error),
    }
}

#[cfg(test)]
mod tests {
    use automerge::{transaction::Transactable, AutoCommit, ReadDoc};
    use tempfile::tempdir;

    use crate::DocumentId;

    use super::FsStore;

    #[test]
    fn compac_adds_new_changes_to_fs() {
        let mut doc = AutoCommit::new();
        doc.put(automerge::ROOT, "foo", "bar").unwrap();

        let data_dir = tempdir().unwrap();

        let doc_id = DocumentId::random();
        let fs = FsStore::open(&data_dir).unwrap();
        let change = doc
            .get_changes(&[])
            .into_iter()
            .flat_map(|c| c.raw_bytes().to_vec())
            .collect::<Vec<_>>();

        fs.append(&doc_id, &change).unwrap();

        doc.put(automerge::ROOT, "foo", "baz").unwrap();
        let compacted = doc.save();

        fs.compact(&doc_id, &compacted).unwrap();

        let reloaded_raw = fs.get(&doc_id).unwrap().unwrap();
        let reloaded = AutoCommit::load(&reloaded_raw).unwrap();
        assert_eq!(
            reloaded.get(&automerge::ROOT, "foo").unwrap().unwrap().0,
            "baz".into()
        );
    }
}
