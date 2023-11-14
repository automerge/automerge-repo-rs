use crate::{DocumentId, Message, RepoId, RepoMessage};

impl Message {
    pub fn decode(data: &[u8]) -> Result<Self, DecodeError> {
        let mut decoder = minicbor::Decoder::new(data);
        let mut sender_id: Option<RepoId> = None;
        let mut target_id: Option<RepoId> = None;
        let mut document_id: Option<DocumentId> = None;
        let mut type_name: Option<&str> = None;
        let mut message: Option<Vec<u8>> = None;
        let len = decoder.map()?.ok_or(DecodeError::MissingLen)?;
        for _ in 0..len {
            let k = decoder.str()?;
            match k {
                //match decoder.str()? {
                "senderId" => sender_id = Some(decoder.str()?.into()),
                "targetId" => target_id = Some(decoder.str()?.into()),
                "channelId" => {
                    if decoder.probe().str().is_ok() {
                        let doc_str = decoder.str()?;
                        if doc_str == "sync" {
                            // automerge-repo-network-websocket encodes the channel id as "sync"
                            // for join messages, we just ignore this
                            continue;
                        }
                        document_id = Some(
                            doc_str
                                .parse()
                                .map_err(|_| DecodeError::InvalidDocumentId)?,
                        );
                    }
                }
                "type" => type_name = Some(decoder.str()?),
                "message" => {
                    message = {
                        // automerge-repo-network-websocket encodes the message using CBOR tag 64,
                        // which is a "uint8 typed array". We have to consume this tag or minicbor
                        // chokes.
                        decoder.tag()?;
                        Some(decoder.bytes()?.to_vec())
                    }
                }
                _ => decoder.skip()?,
            }
        }
        match type_name {
            None => Err(DecodeError::MissingType),
            Some("join") => Ok(Self::Join(sender_id.ok_or(DecodeError::MissingSenderId)?)),
            // Some JS implementations are a bit confused about which field contains the message.
            // This code is compatible with either implementation.
            // See https://github.com/automerge/automerge-repo/pull/111
            Some("sync") | Some("message") => Ok(Self::Repo(RepoMessage::Sync {
                from_repo_id: sender_id.ok_or(DecodeError::MissingSenderId)?,
                to_repo_id: target_id.ok_or(DecodeError::MissingTargetId)?,
                document_id: document_id.ok_or(DecodeError::MissingDocumentId)?,
                message: message.ok_or(DecodeError::MissingData)?,
            })),
            Some("peer") => Ok(Self::Peer(sender_id.ok_or(DecodeError::MissingSenderId)?)),
            Some(other) => Err(DecodeError::UnknownType(other.to_string())),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let out: Vec<u8> = Vec::new();
        let mut encoder = minicbor::Encoder::new(out);
        match self {
            Self::Join(repo_id) => {
                encoder.map(3).unwrap();
                encoder.str("type").unwrap();
                encoder.str("join").unwrap();
                encoder.str("senderId").unwrap();
                encoder.str(repo_id.0.as_str()).unwrap();
                encoder.str("channelId").unwrap();
                encoder.str("sync").unwrap();
            }
            Self::Repo(RepoMessage::Sync {
                from_repo_id,
                to_repo_id,
                document_id,
                message,
            }) => {
                encoder.map(5).unwrap();
                encoder.str("type").unwrap();
                encoder.str("message").unwrap();
                encoder.str("senderId").unwrap();
                encoder.str(from_repo_id.0.as_str()).unwrap();
                encoder.str("targetId").unwrap();
                encoder.str(to_repo_id.0.as_str()).unwrap();
                encoder.str("channelId").unwrap();
                encoder.str(document_id.as_uuid_str().as_str()).unwrap();
                encoder.str("message").unwrap();
                encoder.tag(minicbor::data::Tag::Unassigned(64)).unwrap();
                encoder.bytes(message.as_slice()).unwrap();
            }
            Self::Peer(repo_id) => {
                encoder.map(2).unwrap();
                encoder.str("type").unwrap();
                encoder.str("peer").unwrap();
                encoder.str("senderId").unwrap();
                encoder.str(repo_id.0.as_str()).unwrap();
            }
            _ => todo!(),
        }
        encoder.into_writer()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("missing len")]
    MissingLen,
    #[error("{0}")]
    Minicbor(String),
    #[error("no type field")]
    MissingType,
    #[error("no channel_id field")]
    MissingChannelId,
    #[error("no sender_id field")]
    MissingSenderId,
    #[error("no target_id field")]
    MissingTargetId,
    #[error("no document_id field")]
    MissingDocumentId,
    #[error("no data field")]
    MissingData,
    #[error("no broadcast field")]
    MissingBroadcast,
    #[error("unknown type {0}")]
    UnknownType(String),
    #[error("invalid document id")]
    InvalidDocumentId,
}

impl From<minicbor::decode::Error> for DecodeError {
    fn from(e: minicbor::decode::Error) -> Self {
        Self::Minicbor(e.to_string())
    }
}
