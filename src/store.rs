use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use tokio::time::Instant;

use crate::error::{OPERATION_ON_WRONG_TYPE, RespError};
use crate::resp::types::BulkString;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, Mutex};

type Key = BulkString;

enum Value {
    String(BulkString),
    List(Vec<BulkString>),
}

struct ValueWithTtl {
    value: Value,
    expires_at: Option<Instant>,
}

#[derive(Clone)]
pub struct Store {
    state: Arc<Mutex<StoreState>>,
}

#[derive(Eq, PartialEq, Ord, PartialOrd)]
struct KeyExpiry {
    expires_at: Instant,
    key: Key,
}

struct StoreState {
    data: HashMap<Key, ValueWithTtl>,
    expiring_keys_queue: BinaryHeap<Reverse<KeyExpiry>>,
}

impl StoreState {
    fn new() -> Self {
        StoreState {
            data: HashMap::new(),
            expiring_keys_queue: BinaryHeap::new(),
        }
    }

    fn set(&mut self, key: Key, value: BulkString, expires_at: Option<Instant>) {
        self.data.insert(
            key.clone(),
            ValueWithTtl {
                value: Value::String(value),
                expires_at,
            },
        );
        self.add_key_expiry(key, expires_at);
    }

    fn set_if_exists(&mut self, key: Key, value: BulkString, expires_at: Option<Instant>) {
        if let Some(existing_value) = self.data.get_mut(&key).take_if(|existing_value| {
            existing_value
                .expires_at
                .is_none_or(|expires_at| expires_at >= Instant::now())
        }) {
            existing_value.value = Value::String(value);
            existing_value.expires_at = expires_at;
            self.add_key_expiry(key, expires_at);
        }
    }

    fn set_unless_exists(&mut self, key: Key, value: BulkString, expires_at: Option<Instant>) {
        let value = Value::String(value);

        match self.data.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let existing_value = entry.get_mut();

                if existing_value
                    .expires_at
                    .is_some_and(|expires_at| expires_at < Instant::now())
                {
                    existing_value.value = value;
                    existing_value.expires_at = expires_at;
                    self.add_key_expiry(key, expires_at);
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(ValueWithTtl { value, expires_at });
                self.add_key_expiry(key, expires_at);
            }
        };
    }

    fn get(&self, key: &Key) -> Result<Option<BulkString>, RespError> {
        self.data
            .get(key)
            .take_if(|existing_value| {
                existing_value
                    .expires_at
                    .is_none_or(|expires_at| expires_at >= Instant::now())
            })
            .map(|value_with_ttl| match &value_with_ttl.value {
                Value::String(value) => Ok(value.clone()),
                Value::List(_) => Err(OPERATION_ON_WRONG_TYPE),
            })
            .transpose()
    }

    fn add_key_expiry(&mut self, key: Key, expires_at: Option<Instant>) {
        if let Some(expires_at) = expires_at {
            self.expiring_keys_queue
                .push(Reverse(KeyExpiry { expires_at, key }));
        }
    }

    fn append_to_list(
        &mut self,
        key: Key,
        mut elements: Vec<BulkString>,
    ) -> Result<usize, RespError> {
        Ok(match self.data.entry(key) {
            Entry::Occupied(mut entry) => {
                let existing_value_with_ttl = entry.get_mut();
                match existing_value_with_ttl.value {
                    Value::String(_) => Err(OPERATION_ON_WRONG_TYPE)?,
                    Value::List(ref mut list) => {
                        list.append(&mut elements);
                        list.len()
                    }
                }
            }
            Entry::Vacant(entry) => {
                let elements_len = elements.len();
                entry.insert(ValueWithTtl {
                    value: Value::List(elements),
                    expires_at: None,
                });
                elements_len
            }
        })
    }

    fn vacuum(&mut self) {
        println!("Performing vacuum");
        let now = Instant::now();
        let mut num_keys_removed = 0;

        while let Some(key_expiry) = self.expiring_keys_queue.peek().map(|rev| &rev.0) {
            if key_expiry.expires_at < now {
                if self
                    .data
                    .get(&key_expiry.key)
                    .and_then(|value_with_ttl| value_with_ttl.expires_at)
                    .is_some_and(|expires_at| expires_at < now)
                {
                    self.data.remove(&key_expiry.key);
                    num_keys_removed += 1;
                }

                self.expiring_keys_queue.pop();
            } else {
                break;
            }
        }

        let elapsed_time = now.elapsed();
        println!(
            "Vacuum has removed {num_keys_removed} keys in {} ns",
            elapsed_time.as_nanos()
        )
    }
}

impl Store {
    pub fn new() -> Self {
        Store {
            state: Arc::new(Mutex::new(StoreState::new())),
        }
    }

    pub fn set(&self, key: Key, value: BulkString, expires_at: Option<Instant>) {
        self.state
            .lock()
            .unwrap()
            .set(key.clone(), value, expires_at);
    }

    pub fn set_if_exists(&self, key: Key, value: BulkString, expires_at: Option<Instant>) {
        self.state
            .lock()
            .unwrap()
            .set_if_exists(key, value, expires_at);
    }

    pub fn set_unless_exists(&self, key: Key, value: BulkString, expires_at: Option<Instant>) {
        self.state
            .lock()
            .unwrap()
            .set_unless_exists(key, value, expires_at);
    }

    pub fn get(&self, key: &Key) -> Result<Option<BulkString>, RespError> {
        self.state.lock().unwrap().get(key)
    }

    pub fn append_to_list(&self, key: Key, elements: Vec<BulkString>) -> Result<usize, RespError> {
        self.state.lock().unwrap().append_to_list(key, elements)
    }

    pub fn vacuum(&self) {
        self.state.lock().unwrap().vacuum();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_get_set() {
        let store = Store::new();
        assert_eq!(Ok(None), store.get(&BulkString::from_str("key")));

        store.set(
            BulkString::from_str("key"),
            BulkString::from_str("value"),
            None,
        );

        assert_eq!(
            Ok(Some(BulkString::from_str("value"))),
            store.get(&BulkString::from_str("key"))
        );

        store.set(
            BulkString::from_str("key"),
            BulkString::from_str("value2"),
            None,
        );

        assert_eq!(
            Ok(Some(BulkString::from_str("value2"))),
            store.get(&BulkString::from_str("key"))
        );
    }

    #[test]
    fn test_set_if_exists() {
        let store = Store::new();

        store.set_if_exists(
            BulkString::from_str("key"),
            BulkString::from_str("value"),
            None,
        );
        assert_eq!(Ok(None), store.get(&BulkString::from_str("key")));

        store.set(
            BulkString::from_str("key"),
            BulkString::from_str("value"),
            None,
        );

        store.set_if_exists(
            BulkString::from_str("key"),
            BulkString::from_str("value2"),
            None,
        );

        assert_eq!(
            Ok(Some(BulkString::from_str("value2"))),
            store.get(&BulkString::from_str("key"))
        );
    }

    #[test]
    fn test_set_unless_exists() {
        let store = Store::new();

        store.set_unless_exists(
            BulkString::from_str("key"),
            BulkString::from_str("value"),
            None,
        );
        assert_eq!(
            Ok(Some(BulkString::from_str("value"))),
            store.get(&BulkString::from_str("key"))
        );

        store.set_unless_exists(
            BulkString::from_str("key"),
            BulkString::from_str("value2"),
            None,
        );
        assert_eq!(
            Ok(Some(BulkString::from_str("value"))),
            store.get(&BulkString::from_str("key"))
        );
    }

    #[tokio::test]
    async fn test_get_set_with_ttl() {
        tokio::time::pause();

        let store = Store::new();
        assert_eq!(Ok(None), store.get(&BulkString::from_str("key")));

        store.set(
            BulkString::from_str("key"),
            BulkString::from_str("value"),
            Some(Instant::now() + Duration::from_secs(1)),
        );

        assert_eq!(
            Ok(Some(BulkString::from_str("value"))),
            store.get(&BulkString::from_str("key"))
        );

        tokio::time::advance(Duration::from_secs(2)).await;

        assert_eq!(Ok(None), store.get(&BulkString::from_str("key")));

        store.set(
            BulkString::from_str("key"),
            BulkString::from_str("value"),
            Some(Instant::now() + Duration::from_secs(1)),
        );
    }

    #[tokio::test]
    async fn test_set_if_exists_with_ttl() {
        tokio::time::pause();

        let store = Store::new();
        store.set(
            BulkString::from_str("key"),
            BulkString::from_str("value"),
            Some(Instant::now() + Duration::from_secs(1)),
        );

        store.set_if_exists(
            BulkString::from_str("key"),
            BulkString::from_str("value2"),
            Some(Instant::now() + Duration::from_secs(3)),
        );

        assert_eq!(
            Ok(Some(BulkString::from_str("value2"))),
            store.get(&BulkString::from_str("key"))
        );

        tokio::time::advance(Duration::from_secs(1)).await;

        assert_eq!(
            Ok(Some(BulkString::from_str("value2"))),
            store.get(&BulkString::from_str("key"))
        );

        tokio::time::advance(Duration::from_secs(3)).await;

        store.set_if_exists(
            BulkString::from_str("key"),
            BulkString::from_str("value3"),
            Some(Instant::now() + Duration::from_secs(4)),
        );

        assert_eq!(Ok(None), store.get(&BulkString::from_str("key")));
    }

    #[tokio::test]
    async fn test_set_unless_exists_with_ttl() {
        tokio::time::pause();

        let store = Store::new();
        store.set_unless_exists(
            BulkString::from_str("key"),
            BulkString::from_str("value3"),
            Some(Instant::now() + Duration::from_secs(2)),
        );
        assert_eq!(
            Ok(Some(BulkString::from_str("value3"))),
            store.get(&BulkString::from_str("key"))
        );
        tokio::time::advance(Duration::from_secs(1)).await;

        store.set_unless_exists(
            BulkString::from_str("key"),
            BulkString::from_str("value4"),
            None,
        );
        assert_eq!(
            Ok(Some(BulkString::from_str("value3"))),
            store.get(&BulkString::from_str("key"))
        );
        tokio::time::advance(Duration::from_secs(2)).await;

        store.set_unless_exists(
            BulkString::from_str("key"),
            BulkString::from_str("value4"),
            None,
        );
        assert_eq!(
            Ok(Some(BulkString::from_str("value4"))),
            store.get(&BulkString::from_str("key"))
        );
    }

    #[tokio::test]
    async fn test_vacuum() {
        let key1 = BulkString::from_str("key");
        let value1 = BulkString::from_str("value");

        let key2 = BulkString::from_str("key2");
        let value2 = BulkString::from_str("value2");

        tokio::time::pause();

        let store = Store::new();
        store.set(
            key1.clone(),
            value1.clone(),
            Some(Instant::now() + Duration::from_secs(1)),
        );
        store.set(
            key2.clone(),
            value2.clone(),
            Some(Instant::now() + Duration::from_secs(2)),
        );

        store.vacuum();
        {
            let state = store.state.lock().unwrap();
            assert!(state.data.get(&key1).is_some());
            assert!(state.data.get(&key2).is_some());
            assert_eq!(2, state.expiring_keys_queue.len());
        }

        tokio::time::advance(Duration::from_secs(2)).await;

        assert!(store.get(&key1).unwrap().is_none());
        assert!(store.get(&key2).unwrap().is_some());

        store.vacuum();

        assert!(store.get(&key1).unwrap().is_none());
        assert!(store.get(&key2).unwrap().is_some());
        {
            let state = store.state.lock().unwrap();
            assert!(state.data.get(&key1).is_none());
            assert!(state.data.get(&key2).is_some());
            assert_eq!(1, state.expiring_keys_queue.len());
        }

        store.set(
            key2.clone(),
            value2,
            Some(Instant::now() + Duration::from_secs(2)),
        );

        tokio::time::advance(Duration::from_secs(1)).await;

        store.vacuum();

        assert!(store.get(&key2).unwrap().is_some());
        {
            let state = store.state.lock().unwrap();
            assert!(state.data.get(&key2).is_some());
            assert_eq!(1, state.expiring_keys_queue.len());
        }

        tokio::time::advance(Duration::from_secs(3)).await;

        store.vacuum();

        assert!(store.get(&key2).unwrap().is_none());
        {
            let state = store.state.lock().unwrap();
            assert!(state.data.get(&key2).is_none());
            assert_eq!(0, state.expiring_keys_queue.len());
        }
    }

    #[test]
    fn test_append_to_list() {
        let store = Store::new();

        store.set(
            BulkString::from_str("key"),
            BulkString::from_str("value"),
            None,
        );

        assert_eq!(
            Err(OPERATION_ON_WRONG_TYPE),
            store.append_to_list(
                BulkString::from_str("key"),
                vec![BulkString::from_str("item")]
            )
        );

        assert_eq!(
            Ok(1),
            store.append_to_list(
                BulkString::from_str("list1"),
                vec![BulkString::from_str("item1")]
            )
        );

        assert_eq!(
            Ok(3),
            store.append_to_list(
                BulkString::from_str("list1"),
                vec![BulkString::from_str("item2"), BulkString::from_str("item3")]
            )
        );
    }
}
