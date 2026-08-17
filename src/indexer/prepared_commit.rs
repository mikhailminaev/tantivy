use super::IndexWriter;
use std::time::{Duration, Instant};
use crate::reader::IndexReader;
use crate::schema::document::Document;
use crate::indexer::segment_entry::PublicationGeneration;
use crate::{FutureResult, Opstamp, Searcher, TantivyDocument};

/// A prepared commit
pub struct PreparedCommit<'a, D: Document = TantivyDocument> {
    index_writer: &'a mut IndexWriter<D>,
    payload: Option<String>,
    opstamp: Opstamp,
    publication_generation: PublicationGeneration,
}

/// Breakdown of the durable half of a prepared commit.
#[derive(Clone, Copy, Debug, Default)]
pub struct CommitStats {
    /// Time to enqueue the commit on Tantivy's segment updater.
    pub schedule: Duration,
    /// Time waiting for the scheduled commit to persist its metadata.
    pub wait: Duration,
}

impl<'a, D: Document> PreparedCommit<'a, D> {
    pub(crate) fn new(
        index_writer: &'a mut IndexWriter<D>,
        opstamp: Opstamp,
        publication_generation: PublicationGeneration,
    ) -> Self {
        Self {
            index_writer,
            payload: None,
            opstamp,
            publication_generation,
        }
    }

    /// Returns the opstamp associated with the prepared commit.
    pub fn opstamp(&self) -> Opstamp {
        self.opstamp
    }

    /// Adds an arbitrary payload to the commit.
    pub fn set_payload(&mut self, payload: &str) {
        self.payload = Some(payload.to_string())
    }

    /// Opens an immutable searcher for this prepared commit without persisting it.
    ///
    /// The prepared-commit boundary guarantees that all add operations through this commit's
    /// opstamp have finished indexing and that no later add operation can enter the snapshot.
    /// Deletes through the same opstamp are applied before the searcher is opened. The returned
    /// searcher is independent of `reader`'s current searcher and opening it does not write
    /// `meta.json` or make the commit durable.
    ///
    /// This is intended for a publisher that atomically installs a complete immutable read
    /// generation before handing the same prefix to durable commit processing. Calling
    /// [`Self::abort`] after publishing such a searcher is invalid: it would discard the logical
    /// writer state represented by the published view.
    pub fn open_searcher(&self, reader: &IndexReader) -> crate::Result<Searcher> {
        let snapshot = self
            .index_writer
            .segment_updater()
            .schedule_snapshot_segment_readers_through_generation(
                self.opstamp,
                self.publication_generation,
            )
            .wait()?;
        reader.searcher_for_segment_readers(snapshot.readers)
    }

    /// Rollbacks any change.
    pub fn abort(self) -> crate::Result<Opstamp> {
        self.index_writer.rollback()
    }

    /// Proceeds to commit.
    /// See `.commit_future()`.
    pub fn commit(self) -> crate::Result<Opstamp> {
        self.commit_with_stats().map(|(opstamp, _stats)| opstamp)
    }

    /// Commits and reports scheduler submission separately from the durable
    /// segment-updater wait.
    pub fn commit_with_stats(self) -> crate::Result<(Opstamp, CommitStats)> {
        let schedule_started = Instant::now();
        let commit = self.commit_future();
        let schedule = schedule_started.elapsed();
        let wait_started = Instant::now();
        let opstamp = commit.wait()?;
        Ok((
            opstamp,
            CommitStats {
                schedule,
                wait: wait_started.elapsed(),
            },
        ))
    }

    /// Proceeds to commit.
    ///
    /// Unfortunately, contrary to what `PrepareCommit` may suggests,
    /// this operation is not at all really light.
    /// At this point deletes have not been flushed yet.
    pub fn commit_future(self) -> FutureResult<Opstamp> {
        info!("committing {}", self.opstamp);
        self.index_writer
            .segment_updater()
            .schedule_commit(self.opstamp, self.payload)
    }
}

#[cfg(test)]
mod tests {
    use crate::collector::Count;
    use crate::query::TermQuery;
    use crate::reader::ReloadPolicy;
    use crate::schema::{IndexRecordOption, Schema, STORED, TEXT};
    use crate::{Index, Term};

    #[test]
    fn open_searcher_applies_replacement_before_durable_commit() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field("title", TEXT | STORED);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests()?;

        writer.add_document(doc!(title => "before"))?;
        writer.commit()?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        writer.delete_term(Term::from_field_text(title, "before"));
        writer.add_document(doc!(title => "after"))?;

        let prepared_commit = writer.prepare_commit()?;
        let prepared_searcher = prepared_commit.open_searcher(&reader)?;

        let before_query = TermQuery::new(
            Term::from_field_text(title, "before"),
            IndexRecordOption::Basic,
        );
        let after_query = TermQuery::new(
            Term::from_field_text(title, "after"),
            IndexRecordOption::Basic,
        );

        assert_eq!(prepared_searcher.search(&before_query, &Count)?, 0);
        assert_eq!(prepared_searcher.search(&after_query, &Count)?, 1);
        assert_eq!(reader.searcher().search(&before_query, &Count)?, 1);
        assert_eq!(reader.searcher().search(&after_query, &Count)?, 0);

        prepared_commit.commit()?;
        reader.reload()?;
        assert_eq!(reader.searcher().search(&before_query, &Count)?, 0);
        assert_eq!(reader.searcher().search(&after_query, &Count)?, 1);

        Ok(())
    }
}
