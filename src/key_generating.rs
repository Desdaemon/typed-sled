//! Create [Tree]s with automatically generated keys.
//!
//! # Example
//! ```
//! use typed_sled::key_generating::CounterTree;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // If you want to persist the data use sled::open instead
//!     let db = sled::Config::new().temporary(true).open().unwrap();
//!
//!     // The id is used by sled to identify which Tree in the database (db) to open.
//!     let tree = CounterTree::open(&db, "unique_id");
//!
//!     let (first_key, _) = tree.insert(&5)?;
//!     let (second_key, _) = tree.insert(&6)?;
//!
//!     assert_eq!(first_key, 0);
//!     assert_eq!(second_key, 1);
//!     assert_eq!(tree.get(&0)?, Some(5));
//!     assert_eq!(tree.get(&1)?, Some(6));
//!     Ok(())
//! }
//! ```

use crate::custom_serde::serialize::BincodeSerDe;
use crate::{custom_serde, KV};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Wraps a type that implements KeyGenerating and uses it to
/// generate the keys for a typed_sled::Tree.
///
/// See [`CounterTree`] for a specific example of how to use this type.
pub type KeyGeneratingTree<KG, V, M = BincodeSerDe> =
    crate::custom_serde::key_generating::KeyGeneratingTree<KG, V, M>;

/// Implement on a type that you want to use for key generation
/// for a typed_sled::Tree.
///
/// See CounterTree for a specific example of how to use this trait.
pub use crate::custom_serde::key_generating::KeyGenerating;

pub use crate::custom_serde::key_generating::KeyGeneratingBatch;

/// A typed_sled::Tree with automatically generated and continuously increasing u64 keys.
pub type CounterTree<V, SerDe = BincodeSerDe> = KeyGeneratingTree<Counter, V, SerDe>;

#[derive(Debug, Clone)]
pub struct Counter(Arc<AtomicU64>);

impl<V: KV> KeyGenerating<V> for Counter {
    type Key = u64;
    type SerDe = BincodeSerDe;

    fn initialize(tree: &custom_serde::Tree<Self::Key, V, Self::SerDe>) -> Self {
        if let Some((key, _)) = tree
            .last()
            .expect("KeyGenerating Counter failed to access sled Tree.")
        {
            Counter(Arc::new(AtomicU64::new(key + 1)))
        } else {
            Counter(Arc::new(AtomicU64::new(0)))
        }
    }

    fn next_key(&self) -> Self::Key {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

pub use crate::custom_serde::key_generating::KeyGeneratingTransactionalTree;
