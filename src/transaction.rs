use paste::paste;
use std::{borrow::Borrow, marker::PhantomData};

use sled::transaction::{ConflictableTransactionResult, TransactionResult};

use crate::custom_serde::serialize::{Deserializer, Serializer, Value};
use crate::{custom_serde::serialize, Batch};

pub struct TransactionalTree<'a, K: ?Sized, V, SerDe> {
    inner: &'a sled::transaction::TransactionalTree,
    _key: PhantomData<fn() -> K>,
    _value: PhantomData<fn() -> V>,
    _serde: PhantomData<SerDe>,
}

impl<'a, K, V, SerDe> TransactionalTree<'a, K, V, SerDe> {
    // pub(crate) fn new(sled: &'a sled::transaction::TransactionalTree) -> Self {
    //     Self {
    //         inner: sled,
    //         _key: PhantomData,
    //         _value: PhantomData,
    //         _serde: PhantomData,
    //     }
    // }

    pub fn insert<Q>(
        &self,
        key: &Q,
        value: &V,
    ) -> std::result::Result<
        Option<Value<K, V, SerDe>>,
        sled::transaction::UnabortableTransactionError,
    >
    where
        Q: ?Sized,
        K: Borrow<Q>,
        SerDe: serialize::SerDe<K, V>,
        SerDe::SK: serialize::Serializer<Q>,
    {
        self.inner
            .insert(SerDe::SK::serialize(key), SerDe::SV::serialize(value))
            .map(|opt| opt.map(SerDe::DV::deserialize))
    }

    pub fn remove<Q>(
        &self,
        key: &Q,
    ) -> std::result::Result<
        Option<Value<K, V, SerDe>>,
        sled::transaction::UnabortableTransactionError,
    >
    where
        Q: ?Sized,
        K: Borrow<Q>,
        SerDe: serialize::SerDe<K, V>,
        SerDe::SK: serialize::Serializer<Q>,
    {
        self.inner
            .remove(SerDe::SK::serialize(key))
            .map(|opt| opt.map(SerDe::DV::deserialize))
    }

    pub fn get<Q>(
        &self,
        key: &Q,
    ) -> std::result::Result<
        Option<Value<K, V, SerDe>>,
        sled::transaction::UnabortableTransactionError,
    >
    where
        Q: ?Sized,
        K: Borrow<Q>,
        SerDe: serialize::SerDe<K, V>,
        SerDe::SK: serialize::Serializer<Q>,
    {
        self.inner
            .get(SerDe::SK::serialize(key))
            .map(|opt| opt.map(SerDe::DV::deserialize))
    }

    pub fn apply_batch(
        &self,
        batch: &Batch<K, V, SerDe>,
    ) -> std::result::Result<(), sled::transaction::UnabortableTransactionError> {
        self.inner.apply_batch(&batch.inner)
    }

    pub fn flush(&self) {
        self.inner.flush()
    }

    pub fn generate_id(&self) -> sled::Result<u64> {
        self.inner.generate_id()
    }
}

pub trait Transactional<E = ()> {
    type View<'a>;

    fn transaction<F, A>(&self, f: F) -> TransactionResult<A, E>
    where
        F: for<'a> Fn(Self::View<'a>) -> ConflictableTransactionResult<A, E>;
}

/// Implement this trait for your custom trees to benefit from auto-implementations such as [`Transactional`].
pub trait TreeMeta {
    type Key;
    type Value;
    type SerDe;
    type TransactionView<'view>: View<'view, Tree = Self>;

    fn inner(&self) -> &sled::Tree;
    fn get(&self, key: &Self::Key) -> sled::Result<Option<Self::Value>>;
}

impl TreeMeta for sled::Tree {
    type Key = &'static [u8];
    type Value = sled::IVec;
    type SerDe = ();
    type TransactionView<'view> = &'view sled::transaction::TransactionalTree;

    fn inner(&self) -> &sled::Tree {
        self
    }
    fn get(&self, key: &Self::Key) -> sled::Result<Option<Self::Value>> {
        self.get(key)
    }
}

pub trait View<'view> {
    type Tree;
    fn view(tree: &'view Self::Tree, view: &'view sled::transaction::TransactionalTree) -> Self;
}

impl<'view> View<'view> for &'view sled::transaction::TransactionalTree {
    type Tree = sled::Tree;
    fn view(_: &'view Self::Tree, view: &'view sled::transaction::TransactionalTree) -> Self {
        view
    }
}

macro_rules! impl_transactional {
    ($($Type:ident),+) => {
        impl<Err, $($Type),+> Transactional<Err> for ($(&$Type),+,)
        where
            $($Type: TreeMeta),+
        {
            type View<'view> = ( $($Type::TransactionView<'view>),+, );

            #[allow(non_snake_case)]
            fn transaction<Func, Ret>(&self, f: Func) -> TransactionResult<Ret, Err>
            where
                Func: for<'view> Fn(Self::View<'view>) -> ConflictableTransactionResult<Ret, Err>,
            {
                let ($($Type),+,) = self;
                sled::Transactional::transaction::<_, Ret>(&( $($Type.inner()),+, ), |vars| {
                    paste! {
                        let ($([<$Type _var>]),+,) = vars;
                        f(( $($Type::TransactionView::view($Type, [<$Type _var>])),+, ))
                    }
                })
            }
        }
    };
}

impl_transactional!(A);
impl_transactional!(A, B);
impl_transactional!(A, B, C);
impl_transactional!(A, B, C, D);
impl_transactional!(A, B, C, D, E);
impl_transactional!(A, B, C, D, E, F);
impl_transactional!(A, B, C, D, E, F, G);
impl_transactional!(A, B, C, D, E, F, G, H);
impl_transactional!(A, B, C, D, E, F, G, H, I);
impl_transactional!(A, B, C, D, E, F, G, H, I, J);

#[test]
fn test_multiple_tree_transaction() {
    use crate::Tree;
    let db = sled::Config::new().temporary(true).open().unwrap();
    let tree0 = Tree::<u32, i32>::open(&db, "tree0");
    let tree1 = Tree::<u16, i16>::open(&db, "tree1");
    let tree2 = Tree::<String, i8>::open(&db, "tree2");

    (&tree0, &tree1, &tree2)
        .transaction(|(tree0, tree1, tree2)| {
            tree0.insert(&0, &0)?;
            tree1.insert(&0, &0)?;
            tree2.insert("asd", &0)?;
            // Todo: E in ConflitableTransactionResult<A, E> is not inferred
            // automatically, although Transactional<E = ()> has default E = () type.
            Ok::<_, sled::transaction::ConflictableTransactionError<()>>(())
        })
        .unwrap();

    assert_eq!(tree0.get(&0), Ok(Some(0)));
    assert_eq!(tree1.get(&0), Ok(Some(0)));
}
