//! Create `Tree`s with automatically generated keys.
//!
//! # Example
//! ```
//! use typed_sled::key_generating::CounterTree;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Creating a temporary sled database.
//!     // If you want to persist the data use sled::open instead.
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
use crate::custom_serde::serialize::{self, Value};
use crate::custom_serde::{Batch, Tree};
use crate::transaction::View;
use sled::transaction::{ConflictableTransactionResult, TransactionResult};
use sled::Result;
use std::borrow::Borrow;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};

use super::serialize::Serializer;

/// Wraps a type that implements KeyGenerating and uses it to
/// generate the keys for a [Tree][crate::custom_serde::Tree].
///
/// See CounterTree for a specific example of how to use this type.
#[derive(Clone, Debug)]
pub struct KeyGeneratingTree<KG: KeyGenerating<V>, V, SerDe> {
    key_generator: KG,
    inner: Tree<KG::Key, V, SerDe>,
}

impl<KG, V, SerDe> crate::transaction::TreeMeta for KeyGeneratingTree<KG, V, SerDe>
where
    for<'a> KG: KeyGenerating<V> + 'a,
    for<'a> KG::Key: 'a,
    for<'a> V: 'a,
    for<'a> SerDe: 'a,
{
    type Key = KG::Key;
    type Value = V;
    type SerDe = SerDe;
    type TransactionView<'view> = KeyGeneratingTransactionalTree<'view, KG, V, SerDe>;

    #[inline]
    fn inner(&self) -> &sled::Tree {
        &self.inner.inner
    }
    fn get(&self, key: &Self::Key) -> sled::Result<Option<Self::Value>> {
        self.get(key)
    }
}

pub type KGEntry<K, V, SerDe> = (K, Option<Value<K, V, SerDe>>);

impl<KG: KeyGenerating<V, SerDe = SerDe>, V, SerDe> KeyGeneratingTree<KG, V, SerDe>
where
    SerDe: serialize::SerDe<KG::Key, V>,
{
    pub fn open<T: AsRef<str>>(db: &sled::Db, id: T) -> Self {
        let tree = Tree::open(db, id);
        let key_generator = KG::initialize(&tree);

        Self {
            key_generator,
            inner: tree,
        }
    }

    /// Insert a generated key to a new value, returning the key and the last value if it was set.
    pub fn insert(&self, value: &V) -> Result<KGEntry<KG::Key, V, SerDe>> {
        let key = self.key_generator.next_key();
        let res = self.inner.insert(&key, value);
        res.map(|opt_v| (key, opt_v))
    }

    /// Insert a key to a new value, returning the last value if it was set.
    /// Be careful not to insert a key that conflicts with the keys generated
    /// by the key generator. If you need the generated key for construction of
    /// the value, you can first use `next_key` and then use this method with
    /// the generated key. Alternatively use `insert_fn`.
    pub fn insert_with_key<Q>(&self, key: &Q, value: &V) -> Result<Option<Value<KG::Key, V, SerDe>>>
    where
        Q: ?Sized,
        KG::Key: Borrow<Q>,
        SerDe::SK: Serializer<Q>,
    {
        self.inner.insert(key, value)
    }

    pub fn next_key(&self) -> KG::Key {
        self.key_generator.next_key()
    }

    /// Insert a generated key to a new dynamically created value, returning the key and the last value if it was set.
    /// The argument supplied to `f` is a reference to the key and the returned value is the value that will
    /// be inserted at the key.
    pub fn insert_fn(&self, f: impl Fn(&KG::Key) -> V) -> Result<KGEntry<KG::Key, V, SerDe>>
    where
        SerDe: serialize::SerDe<KG::Key, V>,
    {
        let key = self.key_generator.next_key();
        let value = f(&key);
        let res = self.insert_with_key(&key, &value);
        res.map(|opt_v| (key, opt_v))
    }

    pub fn transaction<F, A, E>(&self, f: F) -> TransactionResult<A, E>
    where
        F: Fn(&KeyGeneratingTransactionalTree<KG, V, SerDe>) -> ConflictableTransactionResult<A, E>,
    {
        self.inner.transaction(|transactional_tree| {
            f(&KeyGeneratingTransactionalTree {
                key_generator: self.key_generator(),
                inner: crate::custom_serde::TransactionalTree::view(
                    &self.inner,
                    &transactional_tree.inner,
                ),
                _marker: PhantomData,
            })
        })
    }

    pub fn key_generator(&self) -> &KG {
        &self.key_generator
    }

    pub fn new_batch(&self) -> KeyGeneratingBatch<KG, V, SerDe> {
        KeyGeneratingBatch {
            key_generator: self.key_generator(),
            inner: Batch::default(),
        }
    }

    pub fn apply_batch(&self, batch: KeyGeneratingBatch<KG, V, SerDe>) -> Result<()> {
        self.inner.apply_batch(batch.inner)
    }
}

impl<KG: KeyGenerating<V>, V, SerDe> Deref for KeyGeneratingTree<KG, V, SerDe> {
    type Target = Tree<KG::Key, V, SerDe>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Implement on a type that you want to use for key generation
/// for a [Tree][crate::custom_serde::Tree].
///
/// See CounterTree for a specific example of how to use this trait.
pub trait KeyGenerating<V> {
    type Key;
    type SerDe: serialize::SerDe<Self::Key, V>;

    fn initialize(tree: &Tree<Self::Key, V, Self::SerDe>) -> Self;

    fn next_key(&self) -> Self::Key;
}

#[derive(Clone, Debug)]
pub struct KeyGeneratingBatch<'a, KG: KeyGenerating<V>, V, SerDe> {
    key_generator: &'a KG,
    inner: Batch<KG::Key, V, SerDe>,
}

impl<'a, KG: KeyGenerating<V, Key = K>, K, V, SerDe> KeyGeneratingBatch<'a, KG, V, SerDe>
where
    SerDe: serialize::SerDe<K, V>,
{
    pub fn insert(&mut self, value: &V) {
        self.inner.insert(&self.key_generator.next_key(), value);
    }

    pub fn remove<Q>(&mut self, key: &Q)
    where
        Q: ?Sized,
        K: Borrow<Q>,
        SerDe::SK: Serializer<Q>,
    {
        self.inner.remove(key)
    }
}

/// A [Tree][crate::custom_serde::Tree] with automatically generated and continuously increasing u64 keys.
pub type CounterTree<V> = KeyGeneratingTree<Counter, V, serialize::BincodeSerDe>;

#[derive(Debug)]
pub struct Counter(AtomicU64);

impl<V> KeyGenerating<V> for Counter
where
    V: crate::KV,
{
    type Key = u64;
    type SerDe = serialize::BincodeSerDe;

    fn initialize(tree: &Tree<Self::Key, V, Self::SerDe>) -> Self {
        if let Some(key) = tree
            .last_key()
            .expect("KeyGenerating Counter failed to access sled Tree.")
        {
            Counter(AtomicU64::new(key + 1))
        } else {
            Counter(AtomicU64::new(0))
        }
    }

    fn next_key(&self) -> Self::Key {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

pub struct KeyGeneratingTransactionalTree<'a, KG: KeyGenerating<V>, V, SerDe> {
    key_generator: &'a KG,
    inner: crate::custom_serde::TransactionalTree<'a, KG::Key, V, SerDe>,
    _marker: PhantomData<(V, SerDe)>,
}

impl<'view, KG: KeyGenerating<V>, V, SerDe> View<'view>
    for KeyGeneratingTransactionalTree<'view, KG, V, SerDe>
{
    type Tree = KeyGeneratingTree<KG, V, SerDe>;

    fn view(tree: &'view Self::Tree, view: &'view sled::transaction::TransactionalTree) -> Self {
        Self {
            key_generator: &tree.key_generator,
            inner: crate::custom_serde::TransactionalTree::new(view),
            _marker: PhantomData,
        }
    }
}

impl<'a, KG: KeyGenerating<V>, V, SerDe> KeyGeneratingTransactionalTree<'a, KG, V, SerDe>
where
    SerDe: serialize::SerDe<KG::Key, V>,
{
    pub fn insert(
        &self,
        value: &V,
    ) -> std::result::Result<
        Option<Value<KG::Key, V, SerDe>>,
        sled::transaction::UnabortableTransactionError,
    > {
        self.inner.insert(&self.key_generator.next_key(), value)
    }

    pub fn apply_batch(
        &self,
        batch: &KeyGeneratingBatch<KG, V, SerDe>,
    ) -> std::result::Result<(), sled::transaction::UnabortableTransactionError> {
        self.inner.apply_batch(&batch.inner)
    }
}

impl<'a, KG: KeyGenerating<V>, V, SerDe> Deref
    for KeyGeneratingTransactionalTree<'a, KG, V, SerDe>
{
    type Target = crate::custom_serde::TransactionalTree<'a, KG::Key, V, SerDe>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
