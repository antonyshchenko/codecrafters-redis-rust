use crate::resp::BulkString;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Store {
    kv_map: Arc<Mutex<HashMap<BulkString, BulkString>>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            kv_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set(&self, key: BulkString, value: BulkString) {
        self.kv_map.lock().unwrap().insert(key, value);
    }

    pub fn get(&self, key: &BulkString) -> Option<BulkString> {
        self.kv_map.lock().unwrap().get(key).cloned()
    }
}
