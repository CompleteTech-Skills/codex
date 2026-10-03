//! Preallocated original causes survive SQLx after-connect retry classification.
use std::sync::Arc;
use std::sync::Mutex;

pub(crate) struct MigrationCauses(Mutex<[Slot; 8]>);
enum Slot {
    Free,
    Reserved,
    Interrupted,
    Failed(sqlx::Error),
}
pub(crate) struct Reservation {
    owner: Arc<MigrationCauses>,
    index: usize,
    finished: bool,
}
impl MigrationCauses {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self(Mutex::new(std::array::from_fn(|_| Slot::Free))))
    }
    pub(crate) fn reserve(self: &Arc<Self>) -> Result<Reservation, sqlx::Error> {
        let mut slots = self.0.lock().map_err(|_| refused())?;
        let index = slots
            .iter()
            .position(|slot| match slot {
                Slot::Free => true,
                Slot::Failed(original) => {
                    let _ = original;
                    false
                }
                _ => false,
            })
            .ok_or_else(refused)?;
        slots[index] = Slot::Reserved;
        Ok(Reservation {
            owner: Arc::clone(self),
            index,
            finished: false,
        })
    }
    #[cfg(test)]
    pub(crate) fn retained_count(&self) -> Result<usize, sqlx::Error> {
        let slots = self.0.lock().map_err(|_| refused())?;
        Ok(slots
            .iter()
            .filter(|slot| match slot {
                Slot::Failed(original) => {
                    let _ = original;
                    true
                }
                Slot::Interrupted => true,
                _ => false,
            })
            .count())
    }
}
fn refused() -> sqlx::Error {
    sqlx::Error::Protocol("original migration cause admission refused".to_string())
}
impl Reservation {
    pub(crate) fn success(mut self) {
        self.owner
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)[self.index] = Slot::Free;
        self.finished = true;
    }
    pub(crate) fn failure(mut self, original: sqlx::Error) -> sqlx::Error {
        self.owner
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)[self.index] =
            Slot::Failed(original);
        self.finished = true;
        refused()
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.finished {
            self.owner
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)[self.index] = Slot::Interrupted;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn interrupted_and_failed_original_slots_cannot_be_overwritten() -> Result<(), sqlx::Error> {
        let owner = MigrationCauses::new();
        drop(owner.reserve()?);
        let _redacted = owner
            .reserve()?
            .failure(sqlx::Error::Protocol("original private detail".to_string()));
        assert_eq!(owner.retained_count()?, 2);
        for _ in 0..6 {
            drop(owner.reserve()?);
        }
        assert!(owner.reserve().is_err());
        assert_eq!(owner.retained_count()?, 8);
        Ok(())
    }
    #[test]
    fn successful_attempt_releases_only_its_own_slot() -> Result<(), sqlx::Error> {
        let owner = MigrationCauses::new();
        drop(owner.reserve()?);
        owner.reserve()?.success();
        assert_eq!(owner.retained_count()?, 1);
        Ok(())
    }
}
