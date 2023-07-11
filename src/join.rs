use std::{borrow::Borrow, sync::Arc};

use crate::{
    custom_serde::serialize::{self, Serializer, Value},
    transaction::TreeMeta,
    Tree,
};

pub trait JoinKeys<Left: TreeMeta, Right: TreeMeta> {
    type Keys<'a, T: 'a>: Iterator<Item = &'a T>
    where
        Self: 'a;
    fn keys(&self, value: &Left::Value) -> Self::Keys<'_, Right::Key>;
}

impl<F, L: TreeMeta, R: TreeMeta> JoinKeys<L, R> for F
where
    F: Fn(&L::Value) -> R::Key,
{
    type Keys<'a, T: 'a> = Option<T>;
    fn keys(&self, value: &L::Value) -> Self::Keys<'_, R::Key> {
        Ok(value).into_iter()
    }
}

// pub trait JoinKeys<Tree: TreeMeta>: TreeMeta + Sized {
//     type Keys<'a, T: 'a>: Iterator<Item = &'a T>
//     where
//         Self: 'a;

//     fn keys(key: &Self::Value) -> Self::Keys<'_, Tree::Key>;
// }

// impl<F, L, R> JoinKeys<R> for F
// where
//     L: TreeMeta,
//     F: Fn(&L) -> R,
// {
//     type Keys<'a, T: 'a> = R;
// }

pub struct JoinTree<Src, Dest> {
    src: Src,
    dest: Dest,
}

pub trait Join {
    type Source;
    type Dest<'a>
    where
        Self: 'a;
    fn join<'a>(
        &self,
        joiner: impl JoinKeys<Self::Source, Self::Dest<'a>>,
    ) -> JoinTree<&Self::Source, Self::Dest<'_>>;
}

impl<'tree, A, B> Join for (&'tree A, &'tree B)
where
    B: TreeMeta,
{
    type Source = A;
    type Dest<'a> = (&'a B,)
    where
        Self: 'a;

    fn join<'a>(
        &self,
        joiner: impl JoinKeys<Self::Source, Self::Dest<'a>>,
    ) -> JoinTree<&Self::Source, Self::Dest<'_>> {
    }
}

impl<'tree, K, V, SerDe, B> JoinTree<&'tree Tree<K, V, SerDe>, (&'tree B,)>
where
    Tree<K, V, SerDe>: JoinKeys<B>,
    B: TreeMeta,
{
    pub fn get<Q>(
        &self,
        key: &Q,
    ) -> sled::Result<
        Option<(
            <Tree<K, V, SerDe> as TreeMeta>::Value,
            Vec<(B::Key, B::Value)>,
        )>,
    >
    where
        Q: ?Sized,
        K: Borrow<Q>,
        SerDe: serialize::SerDe<K, V>,
        SerDe::SK: Serializer<Q>,
        B::Key: Clone,
        V: From<Value<K, V, SerDe>>,
    {
        let src = self.src.get(key)?;
        if let Some(src) = src {
            let mut values = Vec::new();
            for key in Tree::keys(&src) {
                if let Some(value) = self.dest.0.get(key)? {
                    values.push(((*key).clone(), value));
                }
            }
            Ok(Some((src, values)))
        } else {
            Ok(None)
        }
    }
    pub fn get_flat<Q>(
        &self,
        key: &Q,
    ) -> sled::Result<
        Vec<(
            Arc<<Tree<K, V, SerDe> as TreeMeta>::Value>,
            B::Key,
            B::Value,
        )>,
    >
    where
        Q: ?Sized,
        K: Borrow<Q>,
        SerDe: serialize::SerDe<K, V>,
        SerDe::SK: Serializer<Q>,
        B::Key: Clone,
        V: From<Value<K, V, SerDe>>,
    {
        let src = self.src.get(key)?;
        if let Some(src) = src.map(Arc::new) {
            let mut values = Vec::new();
            for key in Tree::keys(&src) {
                if let Some(value) = self.dest.0.get(key)? {
                    values.push((Arc::clone(&src), (*key).clone(), value));
                }
            }
            Ok(values)
        } else {
            Ok(Vec::new())
        }
    }
}

#[test]
fn test() {
    let db = sled::Config::new().temporary(true).open().unwrap();

    let tree1: Tree<u32, (String, u64)> = Tree::open(&db, "tree1");
    let tree2: Tree<u64, u32> = Tree::open(&db, "tree2");

    // if let Ok(Some((value, joined))) = (&tree1, &tree2).join().get(&123) {}
}
