use crate::pb::internal::SnapshotRequest;
use crate::pb::internal::snapshot_request::Payload;
use crate::pb::{self};

impl SnapshotRequest {
    pub fn into_meta(self) -> Option<pb::internal::SnapshotRequestMeta> {
        let p = self.payload?;
        match p {
            Payload::Meta(meta) => Some(meta),
            Payload::Chunk(_) => None,
        }
    }

    pub fn into_data_chunk(self) -> Option<Vec<u8>> {
        let p = self.payload?;
        match p {
            Payload::Meta(_) => None,
            Payload::Chunk(chunk) => Some(chunk),
        }
    }
}
