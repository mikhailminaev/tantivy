use std::fmt;

use common::BitSet;

use crate::index::{SegmentId, SegmentMeta};
use crate::indexer::delete_queue::DeleteCursor;

/// Identifies the publication batch that produced an uncommitted segment.
///
/// A generation is local to one `IndexWriter`. It is not persisted in `meta.json`; after a
/// durable commit its segments become part of the durable base and no longer need this label.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct PublicationGeneration(u64);

impl PublicationGeneration {
    pub(crate) fn new(value: u64) -> Self {
        Self(value)
    }

    pub(crate) fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

/// A segment entry describes the state of
/// a given segment, at a given instant.
///
/// In addition to segment `meta`,
/// it contains a few transient states
/// - `alive_bitset` is a bitset describing documents that were alive during the commit itself.
/// - `delete_cursor` is the position in the delete queue. Deletes happening before the cursor are
///   reflected either in the .del file or in the `alive_bitset`.
#[derive(Clone)]
pub struct SegmentEntry {
    meta: SegmentMeta,
    alive_bitset: Option<BitSet>,
    delete_cursor: DeleteCursor,
    publication_generation: Option<PublicationGeneration>,
}

impl SegmentEntry {
    /// Create a new `SegmentEntry`
    pub fn new(
        segment_meta: SegmentMeta,
        delete_cursor: DeleteCursor,
        alive_bitset: Option<BitSet>,
    ) -> SegmentEntry {
        SegmentEntry {
            meta: segment_meta,
            alive_bitset,
            delete_cursor,
            publication_generation: None,
        }
    }

    /// Creates an uncommitted segment entry owned by one publication generation.
    pub(crate) fn new_for_publication(
        segment_meta: SegmentMeta,
        delete_cursor: DeleteCursor,
        alive_bitset: Option<BitSet>,
        publication_generation: PublicationGeneration,
    ) -> SegmentEntry {
        SegmentEntry {
            meta: segment_meta,
            alive_bitset,
            delete_cursor,
            publication_generation: Some(publication_generation),
        }
    }

    /// Return a reference to the segment entry deleted bitset.
    ///
    /// `DocId` in this bitset are flagged as deleted.
    pub fn alive_bitset(&self) -> Option<&BitSet> {
        self.alive_bitset.as_ref()
    }

    /// Set the `SegmentMeta` for this segment.
    pub fn set_meta(&mut self, segment_meta: SegmentMeta) {
        self.meta = segment_meta;
    }

    /// Return a reference to the segment_entry's delete cursor
    pub fn delete_cursor(&mut self) -> &mut DeleteCursor {
        &mut self.delete_cursor
    }

    /// Returns the segment id.
    pub fn segment_id(&self) -> SegmentId {
        self.meta.id()
    }

    /// Accessor to the `SegmentMeta`
    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    pub(crate) fn publication_generation(&self) -> Option<PublicationGeneration> {
        self.publication_generation
    }

    pub(crate) fn clear_publication_generation(&mut self) {
        self.publication_generation = None;
    }
}

impl fmt::Debug for SegmentEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "SegmentEntry({:?})", self.meta)
    }
}
