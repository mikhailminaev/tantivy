use std::time::{Duration, Instant};

use common::TerminatingWrite;

use crate::directory::WritePtr;
use crate::fieldnorm::FieldNormsSerializer;
use crate::index::{Segment, SegmentComponent};
use crate::postings::InvertedIndexSerializer;
use crate::store::StoreWriter;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SegmentSerializerCreationStats {
    store_open: Duration,
    store_writer_create: Duration,
    fast_fields_open: Duration,
    fieldnorms_output_open: Duration,
    postings_open: Duration,
}

impl SegmentSerializerCreationStats {
    pub(crate) fn store_open_duration(&self) -> Duration {
        self.store_open
    }

    pub(crate) fn store_writer_create_duration(&self) -> Duration {
        self.store_writer_create
    }

    pub(crate) fn fast_fields_open_duration(&self) -> Duration {
        self.fast_fields_open
    }

    pub(crate) fn fieldnorms_output_open_duration(&self) -> Duration {
        self.fieldnorms_output_open
    }

    pub(crate) fn postings_open_duration(&self) -> Duration {
        self.postings_open
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SegmentSerializerCloseStats {
    fieldnorms_close: Duration,
    fast_fields_close: Duration,
    postings_close: Duration,
    store_writer_close: Duration,
}

impl SegmentSerializerCloseStats {
    pub(crate) fn fieldnorms_close_duration(&self) -> Duration {
        self.fieldnorms_close
    }

    pub(crate) fn fast_fields_close_duration(&self) -> Duration {
        self.fast_fields_close
    }

    pub(crate) fn postings_close_duration(&self) -> Duration {
        self.postings_close
    }

    pub(crate) fn store_writer_close_duration(&self) -> Duration {
        self.store_writer_close
    }
}

/// Segment serializer is in charge of laying out on disk
/// the data accumulated and sorted by the `SegmentWriter`.
pub struct SegmentSerializer {
    segment: Segment,
    pub(crate) store_writer: StoreWriter,
    fast_field_write: WritePtr,
    fieldnorms_serializer: Option<FieldNormsSerializer>,
    postings_serializer: InvertedIndexSerializer,
    creation_stats: SegmentSerializerCreationStats,
}

impl SegmentSerializer {
    /// Creates a new `SegmentSerializer`.
    pub fn for_segment(mut segment: Segment) -> crate::Result<SegmentSerializer> {
        let settings = segment.index().settings().clone();
        let (store_writer, store_open, store_writer_create) = {
            let store_open_started = Instant::now();
            let store_write = segment.open_write(SegmentComponent::Store)?;
            let store_open = store_open_started.elapsed();
            let store_writer_create_started = Instant::now();
            let store_writer = StoreWriter::new(
                store_write,
                settings.docstore_compression,
                settings.docstore_blocksize,
                settings.docstore_compress_dedicated_thread,
            )?;
            let store_writer_create = store_writer_create_started.elapsed();
            (store_writer, store_open, store_writer_create)
        };

        let fast_fields_open_started = Instant::now();
        let fast_field_write = segment.open_write(SegmentComponent::FastFields)?;
        let fast_fields_open = fast_fields_open_started.elapsed();

        let fieldnorms_open_started = Instant::now();
        let fieldnorms_write = segment.open_write(SegmentComponent::FieldNorms)?;
        let fieldnorms_serializer = FieldNormsSerializer::from_write(fieldnorms_write)?;
        let fieldnorms_output_open = fieldnorms_open_started.elapsed();

        let postings_open_started = Instant::now();
        let postings_serializer = InvertedIndexSerializer::open(&mut segment)?;
        let postings_open = postings_open_started.elapsed();
        Ok(SegmentSerializer {
            segment,
            store_writer,
            fast_field_write,
            fieldnorms_serializer: Some(fieldnorms_serializer),
            postings_serializer,
            creation_stats: SegmentSerializerCreationStats {
                store_open,
                store_writer_create,
                fast_fields_open,
                fieldnorms_output_open,
                postings_open,
            },
        })
    }

    pub(crate) fn creation_stats(&self) -> SegmentSerializerCreationStats {
        self.creation_stats
    }

    /// The memory used (inclusive childs)
    pub fn mem_usage(&self) -> usize {
        self.store_writer.mem_usage()
    }

    pub fn segment(&self) -> &Segment {
        &self.segment
    }

    /// Accessor to the `PostingsSerializer`.
    pub fn get_postings_serializer(&mut self) -> &mut InvertedIndexSerializer {
        &mut self.postings_serializer
    }

    /// Accessor to the `FastFieldSerializer`.
    pub fn get_fast_field_write(&mut self) -> &mut WritePtr {
        &mut self.fast_field_write
    }

    /// Extract the field norm serializer.
    ///
    /// Note the fieldnorms serializer can only be extracted once.
    pub fn extract_fieldnorms_serializer(&mut self) -> Option<FieldNormsSerializer> {
        self.fieldnorms_serializer.take()
    }

    /// Accessor to the `StoreWriter`.
    pub fn get_store_writer(&mut self) -> &mut StoreWriter {
        &mut self.store_writer
    }

    /// Finalize the segment serialization.
    pub fn close(self) -> crate::Result<()> {
        self.close_with_stats().map(|_| ())
    }

    pub(crate) fn close_with_stats(mut self) -> crate::Result<SegmentSerializerCloseStats> {
        let mut stats = SegmentSerializerCloseStats::default();
        if let Some(fieldnorms_serializer) = self.extract_fieldnorms_serializer() {
            let fieldnorms_close_started = Instant::now();
            fieldnorms_serializer.close()?;
            stats.fieldnorms_close = fieldnorms_close_started.elapsed();
        }
        let fast_fields_close_started = Instant::now();
        self.fast_field_write.terminate()?;
        stats.fast_fields_close = fast_fields_close_started.elapsed();
        let postings_close_started = Instant::now();
        self.postings_serializer.close()?;
        stats.postings_close = postings_close_started.elapsed();
        let store_writer_close_started = Instant::now();
        self.store_writer.close()?;
        stats.store_writer_close = store_writer_close_started.elapsed();
        Ok(stats)
    }
}
