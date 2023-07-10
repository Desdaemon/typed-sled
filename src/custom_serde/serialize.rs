//! Create custom (de)serializers for key and value (de)serialization.
//!
//! The default `Tree` uses bincode for (de)serialization of types
//! that implement DeserializeOwned. However if you want to use
//! zero-copy deserialization, lazy deserialization or simply want
//! to support deserialization of types that don't implement DeserializeOwned
//! you need a different Deserializer. Implementing [SerDe] and
//! using it together with a [Tree][crate::custom_serde::Tree] allows you
//! to do just that.

use std::convert::{TryFrom, TryInto};

// use rkyv::{archived_root, ser::Serializer as _, AlignedVec, Archive, Archived};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// The default `Tree` uses bincode for (de)serialization of types
/// that implement DeserializeOwned. However if you want to use
/// zero-copy deserialization, lazy deserialization or simply want
/// to support deserialization of types that don't implement DeserializeOwned
/// you need a different Deserializer. Implementing this trait and
/// using it together with a [Tree][crate::custom_serde::Tree] allows you
/// to do just that.
pub trait SerDe<K, V> {
    /// Key Serializer
    type SK: Serializer<K>;
    /// Value Serializer
    type SV: Serializer<V>;
    /// Key Deserializer
    type DK: Deserializer<K>;
    /// Value Deserializer
    type DV: Deserializer<V>;
}

pub type Key<K, V, SD> = <<SD as SerDe<K, V>>::DK as Deserializer<K>>::Target<K>;
pub type Value<K, V, SD> = <<SD as SerDe<K, V>>::DV as Deserializer<V>>::Target<V>;

pub trait Serializer<T: ?Sized> {
    type Bytes: AsRef<[u8]> + Into<sled::IVec>;

    fn serialize(value: &T) -> Self::Bytes;
}

pub trait Deserializer<T> {
    type Target<T_>;
    fn deserialize(bytes: sled::IVec) -> Self::Target<T>;
}

/// (De)serializer using bincode.
#[derive(Debug)]
pub struct BincodeSerDe;
pub trait BincodeSerDeBounds: Serialize + DeserializeOwned {}
impl<T> BincodeSerDeBounds for T where T: Serialize + DeserializeOwned {}
#[derive(Debug)]
pub struct BincodeSerDeLazy;
#[derive(Debug)]
pub struct BincodeSerDeLazyK;
#[derive(Debug)]
pub struct BincodeSerDeLazyV;
#[derive(Debug)]
pub struct BincodeSerializer;
#[derive(Debug)]
pub struct BincodeDeserializer;
#[derive(Debug)]
pub struct BincodeDeserializerLazy;

impl<K, V> SerDe<K, V> for BincodeSerDe
where
    K: Serialize + for<'de> Deserialize<'de>,
    V: Serialize + for<'de> Deserialize<'de>,
{
    type SK = BincodeSerializer;
    type SV = BincodeSerializer;
    type DK = BincodeDeserializer;
    type DV = BincodeDeserializer;
}

impl<'limited, K, V> SerDe<K, V> for BincodeSerDeLazy
where
    K: Serialize + Deserialize<'limited>,
    V: Serialize + Deserialize<'limited>,
{
    type SK = BincodeSerializer;
    type SV = BincodeSerializer;
    type DK = BincodeDeserializerLazy;
    type DV = BincodeDeserializerLazy;
}

impl<'limited, K, V> SerDe<K, V> for BincodeSerDeLazyK
where
    K: Serialize + Deserialize<'limited>,
    V: Serialize + for<'de> Deserialize<'de>,
{
    type SK = BincodeSerializer;
    type SV = BincodeSerializer;
    type DK = BincodeDeserializerLazy;
    type DV = BincodeDeserializer;
}

impl<'limited, K, V> SerDe<K, V> for BincodeSerDeLazyV
where
    K: Serialize + for<'de> Deserialize<'de>,
    V: Serialize + Deserialize<'limited>,
{
    type SK = BincodeSerializer;
    type SV = BincodeSerializer;
    type DK = BincodeDeserializer;
    type DV = BincodeDeserializerLazy;
}

impl<T: Serialize + ?Sized> Serializer<T> for BincodeSerializer {
    type Bytes = Vec<u8>;

    fn serialize(value: &T) -> Self::Bytes {
        bincode::serialize(value).expect("serialization failed, did the type serialized change?")
    }
}

impl<T> Deserializer<T> for BincodeDeserializer
where
    T: for<'de> Deserialize<'de>,
{
    type Target<Inner> = Inner;

    fn deserialize(bytes: sled::IVec) -> Self::Target<T> {
        bincode::deserialize(&bytes)
            .expect("deserialization failed, did the type serialized change?")
    }
}

impl<'limited, T> Deserializer<T> for BincodeDeserializerLazy
where
    T: Deserialize<'limited>,
{
    type Target<Inner> = Lazy<Inner>;

    fn deserialize(bytes: sled::IVec) -> Self::Target<T> {
        Lazy::new(bytes)
    }
}

pub struct Lazy<T> {
    v: sled::IVec,
    _t: std::marker::PhantomData<fn() -> T>,
}

impl<T> Lazy<T> {
    fn new(v: sled::IVec) -> Self {
        Self {
            v,
            _t: std::marker::PhantomData,
        }
    }
}

impl<T> Serializer<Lazy<T>> for BincodeSerDeLazy {
    type Bytes = Vec<u8>;

    #[inline]
    fn serialize(value: &Lazy<T>) -> Self::Bytes {
        value.v.to_vec()
    }
}
impl<T> Serializer<Lazy<T>> for BincodeSerDeLazyK {
    type Bytes = Vec<u8>;

    #[inline]
    fn serialize(value: &Lazy<T>) -> Self::Bytes {
        value.v.to_vec()
    }
}
impl<T> Serializer<Lazy<T>> for BincodeSerDeLazyV {
    type Bytes = Vec<u8>;

    #[inline]
    fn serialize(value: &Lazy<T>) -> Self::Bytes {
        value.v.to_vec()
    }
}

impl<T> Lazy<T> {
    /// Deserializes the lazy value via [bincode].
    pub fn deserialize<'de>(&'de self) -> T
    where
        T: Deserialize<'de>,
    {
        bincode::deserialize(&self.v)
            .expect("deserialization failed, did the type serialized change?")
    }
}

#[test]
fn test_lazy() {
    let ref_str_bytes = sled::IVec::from(
        bincode::serialize::<&str>(&"hello there my darling how has your day been?").unwrap(),
    );
    let l = Lazy::<&str>::new(ref_str_bytes);
    l.deserialize();
}

// TODO: Implement (De)serializers for rkyv.
