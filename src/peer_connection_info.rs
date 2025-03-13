use std::{collections::HashMap, time::SystemTime};

use crate::{DocumentId, PeerDocState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerConnectionInfo {
    pub last_received: Option<SystemTime>,
    pub last_sent: Option<SystemTime>,
    pub docs: HashMap<DocumentId, PeerDocState>,
}
