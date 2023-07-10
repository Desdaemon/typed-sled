use serde::{Deserialize, Serialize};
use tantivy::schema::Schema;
use typed_sled::custom_serde::serialize::BincodeSerDeLazyK;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Creating a temporary sled database.
    // If you want to persist the data use sled::open instead.
    let db = sled::Config::new().temporary(true).open().unwrap();

    // The id is used by sled to identify which Tree in the database (db) to open
    let tree = typed_sled::Tree::<String, SomeValue>::open(&db, "unique_id");

    tree.insert(&"some_key".to_owned(), &SomeValue(10))?;

    assert_eq!(tree.get(&"some_key".to_owned())?, Some(SomeValue(10)));
    Ok(())
}

fn lazy_search() -> Result<(), Box<dyn std::error::Error>> {
    let db = sled::Config::new().temporary(true).open()?;
    let tree = typed_sled::Tree::<&str, SomeValue, BincodeSerDeLazyK>::open(&db, "lazy");

    tree.insert("foo", &SomeValue(42))?;

    let mut builder = Schema::builder();
    let _0 = builder.add_u64_field("_0", tantivy::schema::INDEXED);

    let engine = typed_sled::search::SearchEngine::new_temp(
        &tree,
        builder,
        move |_, v| tantivy::doc!(_0 => v.0),
    )?;

    let results = engine.search("42", 10)?;
    assert!(matches!(&results[..], [(_, Some((_, SomeValue(42))))]));

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SomeValue(#[serde(rename = "_0")] u64);
