use crate::{error::Error, event::verifiable_event::VerifiableEvent};
use ::redb::Database;
use keri_core::{database::redb::WriteTxnMode, prefix::IdentifierPrefix};
use said::SelfAddressingIdentifier;
use std::{path::Path, sync::Arc};
pub(crate) mod digest_key_database;
pub mod redb;

pub trait TelEventDatabase {
    fn new(path: impl AsRef<Path>) -> Result<Self, Error>
    where
        Self: Sized;

    fn add_new_event(&self, event: VerifiableEvent, id: &IdentifierPrefix) -> Result<(), Error>;

    fn get_events(
        &self,
        id: &IdentifierPrefix,
    ) -> Option<impl DoubleEndedIterator<Item = VerifiableEvent>>;

    fn get_management_events(
        &self,
        id: &IdentifierPrefix,
    ) -> Option<impl DoubleEndedIterator<Item = VerifiableEvent>>;
}

pub trait TelLogDatabase {
    fn log_event(&self, event: &VerifiableEvent, transaction: &WriteTxnMode) -> Result<(), Error>;
    fn get(&self, digest: &SelfAddressingIdentifier) -> Result<Option<VerifiableEvent>, Error>;
}

pub struct EscrowDatabase(pub(crate) Arc<Database>);

impl EscrowDatabase {
    pub fn new(file_path: &Path) -> Result<Self, Error> {
        keri_core::database::storage::ensure_parent(file_path)
            .map_err(|e| Error::EscrowDatabaseError(e.to_string()))?;
        let db = keri_core::database::storage::open(file_path)
            .map_err(|e| Error::EscrowDatabaseError(e.to_string()))?;
        Ok(Self(Arc::new(db)))
    }
}
