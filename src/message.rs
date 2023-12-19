use std::num::NonZeroU64;

use minicbor::encode::Write;

use crate::{
    interfaces::{EphemeralSessionId, ProtocolVersion},
    DocumentId, Message, RepoId, RepoMessage,
};

impl Message {
    pub fn decode(data: &[u8]) -> Result<Self, DecodeError> {
        let mut decoder = minicbor::Decoder::new(data);
        let mut sender_id: Option<RepoId> = None;
        let mut target_id: Option<RepoId> = None;
        let mut document_id: Option<DocumentId> = None;
        let mut type_name: Option<&str> = None;
        let mut data: Option<Vec<u8>> = None;
        let mut ephemeral_session: Option<EphemeralSessionId> = None;
        let mut ephemeral_count: Option<NonZeroU64> = None;
        let mut err_message: Option<String> = None;
        let mut supported_protocol_versions: Option<Vec<ProtocolVersion>> = None;
        let mut selected_protocol_version: Option<ProtocolVersion> = None;
        let len = decoder.map()?.ok_or(DecodeError::MissingLen)?;
        for _ in 0..len {
            let k = decoder.str()?;
            match k {
                //match decoder.str()? {
                "senderId" => sender_id = Some(decoder.str()?.into()),
                "targetId" => target_id = Some(decoder.str()?.into()),
                "documentId" => {
                    if decoder.probe().str().is_ok() {
                        let doc_id_str = decoder.str()?;
                        document_id = Some(
                            doc_id_str
                                .parse()
                                .map_err(|_| DecodeError::InvalidDocumentId)?,
                        );
                    } else {
                        document_id = Some(
                            DocumentId::try_from(decoder.bytes()?.to_vec())
                                .map_err(|_| DecodeError::InvalidDocumentId)?,
                        )
                    }
                }
                "type" => type_name = Some(decoder.str()?),
                "data" => data = Some(decoder.bytes()?.to_vec()),
                "sessionId" => ephemeral_session = Some(decoder.str()?.into()),
                "count" => {
                    ephemeral_count = Some(
                        NonZeroU64::new(decoder.u64()?)
                            .ok_or(DecodeError::NonPositiveEphemeralCount)?,
                    )
                }
                "selectedProtocolVersion" => {
                    selected_protocol_version = {
                        let raw = decoder.str()?;
                        Some(ProtocolVersion::from(raw.to_string()))
                    }
                }
                "supportedProtocolVersions" => {
                    let seq = decoder.array_iter::<String>()?;
                    supported_protocol_versions = Some(
                        seq.map(|raw| raw.map(ProtocolVersion::from))
                            .collect::<Result<Vec<_>, _>>()?,
                    )
                }
                "message" => err_message = Some(decoder.str()?.into()),
                _ => decoder.skip()?,
            }
        }
        match type_name {
            None => Err(DecodeError::MissingType),
            Some("join") => Ok(Self::Join {
                sender: sender_id.ok_or(DecodeError::MissingSenderId)?,
                supported_protocol_versions: supported_protocol_versions
                    .ok_or(DecodeError::MissingSupportedProtocolVersions)?,
            }),
            Some("peer") => Ok(Self::Peer {
                sender: sender_id.ok_or(DecodeError::MissingSenderId)?,
                selected_protocol_version: selected_protocol_version
                    .ok_or(DecodeError::MissingSelectedProtocolVersion)?,
            }),
            Some("sync") => Ok(Self::Repo(RepoMessage::Sync {
                from_repo_id: sender_id.ok_or(DecodeError::MissingSenderId)?,
                to_repo_id: target_id.ok_or(DecodeError::MissingTargetId)?,
                document_id: document_id.ok_or(DecodeError::MissingDocumentId)?,
                message: data.ok_or(DecodeError::MissingData)?,
            })),
            Some("ephemeral") => Ok(Self::Repo(RepoMessage::Ephemeral {
                from_repo_id: sender_id.ok_or(DecodeError::MissingSenderId)?,
                to_repo_id: target_id.ok_or(DecodeError::MissingTargetId)?,
                document_id: document_id.ok_or(DecodeError::MissingDocumentId)?,
                session_id: ephemeral_session.ok_or(DecodeError::MissingEphemeralSession)?,
                count: ephemeral_count.ok_or(DecodeError::MissingEphemeralCount)?,
                message: data.ok_or(DecodeError::MissingData)?,
            })),
            Some("request") => Ok(Self::Repo(RepoMessage::Request {
                sender_id: sender_id.ok_or(DecodeError::MissingSenderId)?,
                target_id: target_id.ok_or(DecodeError::MissingTargetId)?,
                document_id: document_id.ok_or(DecodeError::MissingDocumentId)?,
                sync_message: data.ok_or(DecodeError::MissingData)?,
            })),
            Some("doc-unavailable") => Ok(Self::Repo(RepoMessage::Unavailable {
                document_id: document_id.ok_or(DecodeError::MissingDocumentId)?,
                sender_id: sender_id.ok_or(DecodeError::MissingSenderId)?,
                target_id: target_id.ok_or(DecodeError::MissingTargetId)?,
            })),
            Some("error") => Ok(Self::Error {
                message: err_message.ok_or(DecodeError::MissingErrorMessage)?,
            }),
            Some(other) => Err(DecodeError::UnknownType(other.to_string())),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let out: Vec<u8> = Vec::new();
        let mut encoder = minicbor::Encoder::new(out);
        match self {
            Self::Join {
                sender,
                supported_protocol_versions,
            } => {
                encoder.map(3).unwrap();
                encoder.str("type").unwrap();
                encoder.str("join").unwrap();
                encoder.str("senderId").unwrap();
                encoder.str(sender.0.as_str()).unwrap();
                encoder.str("supportedProtocolVersions").unwrap();
                encoder
                    .array(supported_protocol_versions.len() as u64)
                    .unwrap();
                for version in supported_protocol_versions {
                    encoder.str(version.as_ref()).unwrap();
                }
            }
            Self::Peer {
                sender: repo_id,
                selected_protocol_version,
            } => {
                encoder.map(3).unwrap();
                encoder.str("type").unwrap();
                encoder.str("peer").unwrap();
                encoder.str("selectedProtocolVersion").unwrap();
                encoder.str(selected_protocol_version.as_ref()).unwrap();
                encoder.str("senderId").unwrap();
                encoder.str(repo_id.0.as_str()).unwrap();
            }
            Self::Repo(RepoMessage::Request {
                sender_id,
                target_id,
                document_id,
                sync_message,
            }) => {
                encoder.map(5).unwrap();
                encoder.str("type").unwrap();
                encoder.str("request").unwrap();
                encoder.str("senderId").unwrap();
                encoder.str(sender_id.0.as_str()).unwrap();
                encoder.str("targetId").unwrap();
                encoder.str(target_id.0.as_str()).unwrap();
                encoder.str("documentId").unwrap();
                encode_doc_id(&mut encoder, document_id);
                encoder.str("data").unwrap();
                encoder.bytes(sync_message.as_slice()).unwrap();
            }
            Self::Repo(RepoMessage::Sync {
                from_repo_id,
                to_repo_id,
                document_id,
                message,
            }) => {
                encoder.map(5).unwrap();
                encoder.str("type").unwrap();
                encoder.str("sync").unwrap();
                encoder.str("senderId").unwrap();
                encoder.str(from_repo_id.0.as_str()).unwrap();
                encoder.str("targetId").unwrap();
                encoder.str(to_repo_id.0.as_str()).unwrap();
                encoder.str("documentId").unwrap();
                encode_doc_id(&mut encoder, document_id);
                encoder.str("data").unwrap();
                encoder.bytes(message.as_slice()).unwrap();
            }
            Self::Repo(RepoMessage::Ephemeral {
                from_repo_id,
                to_repo_id,
                document_id,
                message,
                session_id,
                count,
            }) => {
                encoder.map(7).unwrap();
                encoder.str("type").unwrap();
                encoder.str("ephemeral").unwrap();
                encoder.str("senderId").unwrap();
                encoder.str(from_repo_id.0.as_str()).unwrap();
                encoder.str("targetId").unwrap();
                encoder.str(to_repo_id.0.as_str()).unwrap();
                encoder.str("documentId").unwrap();
                encode_doc_id(&mut encoder, document_id);
                encoder.str("sessionId").unwrap();
                encoder.str(session_id.as_ref()).unwrap();
                encoder.str("data").unwrap();
                encoder.bytes(message.as_slice()).unwrap();
                encoder.str("count").unwrap();
                encoder.u64(u64::from(*count)).unwrap();
            }
            Self::Repo(RepoMessage::Unavailable {
                sender_id,
                target_id,
                document_id,
            }) => {
                encoder.map(4).unwrap();
                encoder.str("type").unwrap();
                encoder.str("doc-unavailable").unwrap();
                encoder.str("senderId").unwrap();
                encoder.str(sender_id.0.as_str()).unwrap();
                encoder.str("targetId").unwrap();
                encoder.str(target_id.0.as_str()).unwrap();
                encoder.str("documentId").unwrap();
                encode_doc_id(&mut encoder, document_id);
            }
            Self::Error { message } => {
                encoder.map(2).unwrap();
                encoder.str("type").unwrap();
                encoder.str("error").unwrap();
                encoder.str("message").unwrap();
                encoder.str(message).unwrap();
            }
        }
        encoder.into_writer()
    }
}

fn encode_doc_id<W: Write>(encoder: &mut minicbor::Encoder<W>, doc_id: &DocumentId)
where
    W::Error: std::fmt::Debug,
{
    encoder.str(format!("{}", doc_id).as_str()).unwrap();
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("missing len")]
    MissingLen,
    #[error("{0}")]
    Minicbor(String),
    #[error("no type field")]
    MissingType,
    #[error("no documentId field")]
    MissingDocumentId,
    #[error("no sender_id field")]
    MissingSenderId,
    #[error("no target_id field")]
    MissingTargetId,
    #[error("no document_id field")]
    MissingData,
    #[error("no broadcast field")]
    MissingBroadcast,
    #[error("unknown type {0}")]
    UnknownType(String),
    #[error("non-positive ephemeral session count")]
    NonPositiveEphemeralCount,
    #[error("no ephemeral session id")]
    MissingEphemeralSession,
    #[error("no ephemeral session count")]
    MissingEphemeralCount,
    #[error("no error message")]
    MissingErrorMessage,
    #[error("missing selectedProtocolVersion")]
    MissingSelectedProtocolVersion,
    #[error("missing supportedProtocolVersions")]
    MissingSupportedProtocolVersions,
    #[error("invalid document id")]
    InvalidDocumentId,
}

impl From<minicbor::decode::Error> for DecodeError {
    fn from(e: minicbor::decode::Error) -> Self {
        Self::Minicbor(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use bolero::check;

    use super::Message;

    #[test]
    fn test_message_encode_decode() {
        check!().with_arbitrary::<Message>().for_each(|message| {
            let encoded = message.encode();
            let decoded = Message::decode(&encoded).unwrap();
            assert_eq!(message, &decoded);
        })
    }
}
