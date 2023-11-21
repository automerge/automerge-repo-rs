use std::collections::{HashMap, HashSet};

use automerge::sync::Message as SyncMessage;

use crate::{DocumentId, RepoId};

#[derive(Debug)]
pub(super) struct Request {
    document_id: DocumentId,
    awaiting_response_from: HashSet<RepoId>,
    awaiting_our_response: HashMap<RepoId, SyncMessage>,
}

impl Request {
    pub(super) fn new(doc_id: DocumentId) -> Self {
        Request {
            document_id: doc_id,
            awaiting_response_from: HashSet::new(),
            awaiting_our_response: HashMap::new(),
        }
    }

    pub(super) fn document_id(&self) -> &DocumentId {
        &self.document_id
    }

    pub(super) fn mark_unavailable(&mut self, repo_id: &RepoId) {
        self.awaiting_our_response.remove(repo_id);
        self.awaiting_response_from.remove(repo_id);
    }

    pub(super) fn is_complete(&self) -> bool {
        self.awaiting_response_from.is_empty()
    }

    pub(super) fn initiate_local<'a, I: Iterator<Item = &'a RepoId>>(
        &mut self,
        connected_peers: I,
    ) -> HashSet<RepoId> {
        self.initiate_inner(None, connected_peers)
    }

    pub(super) fn initiate_remote<'a, I: Iterator<Item = &'a RepoId>>(
        &mut self,
        from_peer: &RepoId,
        request_sync_message: SyncMessage,
        connected_peers: I,
    ) -> HashSet<RepoId> {
        self.initiate_inner(Some((from_peer, request_sync_message)), connected_peers)
    }

    fn initiate_inner<'a, I: Iterator<Item = &'a RepoId>>(
        &mut self,
        from_repo_id: Option<(&RepoId, SyncMessage)>,
        connected_peers: I,
    ) -> HashSet<RepoId> {
        if let Some((from_peer, initial_message)) = from_repo_id {
            self.awaiting_our_response
                .insert(from_peer.clone(), initial_message);
        }
        connected_peers
            .filter(|remote| {
                if self.awaiting_our_response.contains_key(remote)
                    || self.awaiting_response_from.contains(remote)
                {
                    false
                } else {
                    self.awaiting_response_from.insert((*remote).clone());
                    true
                }
            })
            .cloned()
            .collect()
    }

    pub(super) fn fulfilled(self) -> HashMap<RepoId, SyncMessage> {
        self.awaiting_our_response
    }

    pub(super) fn unavailable(self) -> impl Iterator<Item = RepoId> {
        self.awaiting_our_response.into_keys()
    }
}
