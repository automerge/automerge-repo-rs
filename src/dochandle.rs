use crate::repo::CollectionEvent;
use crate::repo::{CollectionId, DocumentId};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::{Mutex, Notify};

/// The doc handle state machine.
#[derive(Clone, Debug)]
pub(crate) enum DocState {
    Start,
    Ready,
}

#[derive(Clone, Debug)]
pub struct DocHandle {
    state: Arc<Mutex<DocState>>,
    notify: Arc<Notify>,
    collection_sender: Sender<(CollectionId, CollectionEvent)>,
    document_id: DocumentId,
    collection_id: CollectionId,
}

impl DocHandle {
    pub(crate) fn new(
        collection_sender: Sender<(CollectionId, CollectionEvent)>,
        document_id: DocumentId,
        collection_id: CollectionId,
    ) -> Self {
        let state = DocState::Start;
        DocHandle {
            state: Arc::new(Mutex::new(state)),
            notify: Arc::new(Notify::new()),
            collection_sender,
            document_id,
            collection_id,
        }
    }

    pub async fn change(&self) {
        self.collection_sender
            .send((
                self.collection_id.clone(),
                CollectionEvent::DocChange(self.document_id.clone()),
            ))
            .await
            .expect("Failed to send doc change event.");
    }

    pub async fn set_ready(&self) {
        {
            let mut state = self.state.lock().await;
            *state = DocState::Ready;
        }
        self.notify.notify_one();
    }

    pub async fn wait_ready(&self) {
        let future = self.notify.notified();
        tokio::pin!(future);

        loop {
            future.as_mut().enable();

            {
                let state = &*self.state.lock().await;
                match state {
                    DocState::Start => {}
                    DocState::Ready => return,
                }
            }

            future.as_mut().await;
            future.set(self.notify.notified());
        }
    }
}
