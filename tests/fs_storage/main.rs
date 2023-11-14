use automerge::transaction::Transactable;
use automerge_repo::fs_store;
use itertools::Itertools;

/// Asserts that the &[u8] in `data` is some permutation of the chunks of Vec<&[u8> in `expected`
macro_rules! assert_permutation_of {
    ($data:expr, $expected:expr) => {
        let data = $data;
        let expected = $expected
            .clone()
            .into_iter()
            .map(|x| x.to_vec())
            .collect_vec();
        let perms = permutations(expected);
        if !perms.contains(&data) {
            panic!("expected {:?} to be a permutation of {:?}", data, $expected);
        }
    };
}

#[test]
fn fs_store_crud() {
    tracing_subscriber::fmt::init();
    let temp_dir = tempfile::TempDir::new().unwrap();
    let store = fs_store::FsStore::open(temp_dir.path()).unwrap();

    let mut doc = automerge::AutoCommit::new();

    doc.put(automerge::ObjId::Root, "key", "value").unwrap();
    let mut change1 = doc.get_last_local_change().unwrap().clone();

    let doc_id = automerge_repo::DocumentId::random();
    store.append(&doc_id, change1.bytes().as_ref()).unwrap();
    let result = store.get(&doc_id).unwrap().unwrap();
    assert_eq!(&result, change1.bytes().as_ref());

    // now append again
    doc.put(automerge::ObjId::Root, "key2", "value2").unwrap();
    let mut change2 = doc.get_last_local_change().unwrap().clone();

    store.append(&doc_id, change2.bytes().as_ref()).unwrap();

    // check that list is working
    let result = store.list().unwrap();
    let expected = &[doc_id.clone()];
    assert_eq!(&result, expected);

    let result = store.get(&doc_id).unwrap().unwrap();
    assert_permutation_of!(result, vec![change1.bytes(), change2.bytes()]);

    // now compact
    store.compact(&doc_id, &[]).unwrap();
    let result = store.get(&doc_id).unwrap().unwrap();
    let expected = doc.save();
    assert_eq!(result, expected);

    // check nonexistent docs don't upset anyone
    let nonexistent_doc_id = automerge_repo::DocumentId::random();
    let result = store.get(&nonexistent_doc_id).unwrap();
    assert!(result.is_none());
}

fn permutations(chunks: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    let num_chunks = chunks.len();
    chunks
        .into_iter()
        .permutations(num_chunks)
        .unique()
        .map(|x| x.into_iter().flatten().collect::<Vec<u8>>())
        .collect()
}
