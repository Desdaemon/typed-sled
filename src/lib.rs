//! typed-sled - a database build on top of sled.
//!
//! sled is a high-performance embedded database with an API that is similar to a `BTreeMap<[u8], [u8]>`.  
//! typed-sled builds on top of sled and offers an API that is similar to a `BTreeMap<K, V>`, where
//! K and V are user defined types which implement [Deserialize][serde::Deserialize] and [Serialize][serde::Serialize].
//!
//! # features
//! Multiple features for common use cases are also available:
//! * [search]: `SearchEngine` on top of a `Tree`.
//! * [key_generating]: Create `Tree`s with automatically generated keys.
//! * [convert]: Convert any `Tree` into another `Tree` with different key and value types.
//! * [custom_serde]: Create `Tree`s with custom (de)serialization. This for example makes
//!                   lazy or zero-copy (de)serialization possible.
//!
//! # Example
//! ```
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
//! struct SomeValue(u32);
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Creating a temporary sled database.
//!     // If you want to persist the data use sled::open instead.
//!     let db = sled::Config::new().temporary(true).open().unwrap();
//!
//!     // The id is used by sled to identify which Tree in the database (db) to open
//!     let tree = typed_sled::Tree::<String, SomeValue>::open(&db, "unique_id");
//!
//!     tree.insert(&"some_key".to_owned(), &SomeValue(10))?;
//!
//!     assert_eq!(tree.get(&"some_key".to_owned())?, Some(SomeValue(10)));
//!     Ok(())
//! }
//! ```
//! [sled]: https://docs.rs/sled/latest/sled/

#![deny(clippy::type_complexity)]

#[doc(inline)]
pub use sled::{open, Config};

#[cfg(any(doc, feature = "convert"))]
pub mod convert;
#[cfg(any(doc, feature = "key-generating"))]
pub mod key_generating;
#[cfg(any(all(doc, feature = "tantivy"), feature = "search"))]
pub mod search;
pub mod transaction;

pub mod custom_serde;

pub mod join;

use core::iter::{DoubleEndedIterator, Iterator};
use serde::Serialize;
use sled::Result;
use std::marker::PhantomData;

#[doc(inline)]
pub use custom_serde::Tree;

/// Trait alias for bounds required on keys and values.
/// For now only types that implement DeserializeOwned
/// are supported.
// [specilization] might make
// supporting any type that implements Deserialize<'a>
// possible without much overhead. Otherwise the branch
// custom_de_serialization introduces custom (de)serialization
// for each `Tree` which might also make it possible.
//
// [specialization]: https://github.com/rust-lang/rust/issues/31844
pub trait KV: serde::de::DeserializeOwned + Serialize {}

impl<T: serde::de::DeserializeOwned + Serialize> KV for T {}

#[doc(inline)]
pub use custom_serde::CompareAndSwapError;

// implemented like this in the sled source
impl<V: std::fmt::Debug> std::error::Error for CompareAndSwapError<V> {}

/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use sled::{Config, IVec};
///
/// fn concatenate_merge(
///   _key: String,               // the key being merged
///   old_value: Option<Vec<f32>>,  // the previous value, if one existed
///   merged_bytes: Vec<f32>        // the new bytes being merged in
/// ) -> Option<Vec<f32>> {       // set the new value, return None to delete
///   let mut ret = old_value
///     .map(|ov| ov.to_vec())
///     .unwrap_or_else(|| vec![]);
///
///   ret.extend_from_slice(&merged_bytes);
///
///   Some(ret)
/// }
///
/// let db = sled::Config::new()
///   .temporary(true).open()?;
///
/// let tree = typed_sled::Tree::<String, Vec<f32>>::open(&db, "unique_id");
/// tree.set_merge_operator(concatenate_merge);
///
/// let k = String::from("some_key");
///
/// tree.insert(&k, &vec![0.0]);
/// tree.merge(&k, &vec![1.0]);
/// tree.merge(&k, &vec![2.0]);
/// assert_eq!(tree.get(&k)?, Some(vec![0.0, 1.0, 2.0]));
///
/// // Replace previously merged data. The merge function will not be called.
/// tree.insert(&k, &vec![3.0]);
/// assert_eq!(tree.get(&k)?, Some(vec![3.0]));
///
/// // Merges on non-present values will cause the merge function to be called
/// // with `old_value == None`. If the merge function returns something (which it
/// // does, in this case) a new value will be inserted.
/// tree.remove(&k);
/// tree.merge(&k, &vec![4.0]);
/// assert_eq!(tree.get(&k)?, Some(vec![4.0]));
/// # Ok(()) }
/// ```
pub trait MergeOperator<K, V>: Fn(K, Option<V>, V) -> Option<V> {}

impl<K, V, F> MergeOperator<K, V> for F where F: Fn(K, Option<V>, V) -> Option<V> {}

/// Iterator implementation for [`Tree`]s.
pub struct Iter<K, V> {
    inner: sled::Iter,
    _key: PhantomData<fn() -> K>,
    _value: PhantomData<fn() -> V>,
}

impl<K: KV, V: KV> Iterator for Iter<K, V> {
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
    }

    fn last(mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
    }
}

impl<K: KV, V: KV> DoubleEndedIterator for Iter<K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
    }
}

impl<K, V> Iter<K, V> {
    pub fn from_sled(iter: sled::Iter) -> Self {
        Iter {
            inner: iter,
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    pub fn keys(self) -> impl DoubleEndedIterator<Item = Result<K>> + Send + Sync
    where
        K: KV + Send + Sync,
        V: KV + Send + Sync,
    {
        self.map(|r| r.map(|(k, _v)| k))
    }

    /// Iterate over the values of this Tree
    pub fn values(self) -> impl DoubleEndedIterator<Item = Result<V>> + Send + Sync
    where
        K: KV + Send + Sync,
        V: KV + Send + Sync,
    {
        self.map(|r| r.map(|(_k, v)| v))
    }
}

#[doc(inline)]
pub use custom_serde::Batch;

#[doc(inline)]
pub use custom_serde::Subscriber;

#[doc(inline)]
pub use custom_serde::Event;

/// The function which is used to deserialize all keys and values.
pub fn deserialize<'a, T>(bytes: &'a [u8]) -> T
where
    T: serde::de::Deserialize<'a>,
{
    bincode::deserialize(bytes).expect("deserialization failed, did the type serialized change?")
}

/// The function which is used to serialize all keys and values.
pub fn serialize<T>(value: &T) -> Vec<u8>
where
    T: serde::Serialize,
{
    bincode::serialize(value).expect("serialization failed, did the type serialized change?")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range() {
        let config = sled::Config::new().temporary(true);
        let db = config.open().unwrap();

        let tree: Tree<u32, u32> = Tree::open(&db, "test_tree");

        tree.insert(&1, &2).unwrap();
        tree.insert(&3, &4).unwrap();
        tree.insert(&6, &2).unwrap();
        tree.insert(&10, &2).unwrap();
        tree.insert(&15, &2).unwrap();
        tree.flush().unwrap();

        let expect_results = [(6, 2), (10, 2)];

        for (i, result) in tree.range(6..11).enumerate() {
            assert_eq!(result.unwrap(), expect_results[i]);
        }
    }

    #[test]
    fn test_cas() {
        let config = sled::Config::new().temporary(true);
        let db = config.open().unwrap();

        let tree: Tree<u32, u32> = Tree::open(&db, "test_tree");

        let current = 2;
        tree.insert(&1, &current).unwrap();
        let expected = 3;
        let proposed = 4;
        let res = tree
            .compare_and_swap(&1, Some(&expected), Some(&proposed))
            .expect("db failure");

        assert_eq!(
            res,
            Err(CompareAndSwapError {
                current: Some(current),
                proposed: Some(proposed),
            }),
        );
    }
}
