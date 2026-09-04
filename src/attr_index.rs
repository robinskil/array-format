//! Attribute index: every visible array's attributes, resolved across layers.
//!
//! Each delta layer interns its own keys and values, so an index in
//! [`ArrayMeta::attributes`](crate::layout::ArrayMeta) only means something
//! against the layer that stores the array. Resolving that per array on every
//! query costs a map lookup and a dictionary scan per array.
//!
//! This index does the translation once, when the file opens. It holds one
//! column per attribute key, so "give me this key for every array" is a single
//! pass over that column.

use std::collections::HashMap;

use indexmap::IndexMap;

use crate::{
    delta::{Delta, DeltaImmutable},
    layout::AttributeValue,
};

/// Locates an array's metadata: `(layer, position in that layer's footer)`.
type ArrayLoc = (u32, u32);

/// Resolved attribute state for the committed layers of a file.
///
/// Array names are not copied. `arrays` points at the footer entry that
/// already owns each name, so a query borrows it.
pub(crate) struct AttrIndex {
    /// Visible arrays in `list_arrays` order. Index into this is the array id.
    arrays: Vec<ArrayLoc>,
    /// Attribute key string to key id.
    key_ids: HashMap<String, u32>,
    /// Attribute values, interned once across every layer.
    values: Vec<AttributeValue>,
    /// One column per key id: `(array_id, value_id)` pairs, sorted by array id.
    columns: Vec<Vec<(u32, u32)>>,
}

impl AttrIndex {
    /// Builds the index from the committed layers, newest layer winning.
    pub(crate) fn build(deltas: &[Delta<DeltaImmutable>]) -> Self {
        // Merge oldest to newest, the way `list_arrays` does, so the index
        // holds the same arrays in the same order.
        let mut visible: IndexMap<&str, ArrayLoc> = IndexMap::new();
        for (layer, delta) in deltas.iter().enumerate() {
            for (pos, meta) in delta.inner.footer.arrays.iter().enumerate() {
                if meta.deleted {
                    visible.shift_remove(meta.name.as_str());
                } else {
                    visible.insert(meta.name.as_str(), (layer as u32, pos as u32));
                }
            }
        }

        let mut key_ids: HashMap<String, u32> = HashMap::new();
        let mut value_ids: HashMap<AttributeValue, u32> = HashMap::new();
        let mut values: Vec<AttributeValue> = Vec::new();
        let mut columns: Vec<Vec<(u32, u32)>> = Vec::new();

        // Translate each layer's dictionaries into the shared one once, rather
        // than once per array that uses them.
        let mut key_map: Vec<Vec<u32>> = Vec::with_capacity(deltas.len());
        let mut value_map: Vec<Vec<u32>> = Vec::with_capacity(deltas.len());
        for delta in deltas {
            let footer = &delta.inner.footer;
            key_map.push(
                footer
                    .attr_keys
                    .iter()
                    .map(|k| match key_ids.get(k) {
                        Some(&id) => id,
                        None => {
                            let id = columns.len() as u32;
                            key_ids.insert(k.clone(), id);
                            columns.push(Vec::new());
                            id
                        }
                    })
                    .collect(),
            );
            value_map.push(
                footer
                    .attr_values
                    .iter()
                    .map(|v| match value_ids.get(v) {
                        Some(&id) => id,
                        None => {
                            let id = values.len() as u32;
                            value_ids.insert(v.clone(), id);
                            values.push(v.clone());
                            id
                        }
                    })
                    .collect(),
            );
        }

        let arrays: Vec<ArrayLoc> = visible.into_values().collect();
        // Arrays are visited in array id order, so every column comes out
        // sorted by array id without a sort.
        for (array_id, &(layer, pos)) in arrays.iter().enumerate() {
            let meta = &deltas[layer as usize].inner.footer.arrays[pos as usize];
            for (key_idx, val_idx) in meta.attributes.iter_entries() {
                let (Some(&key_id), Some(&value_id)) = (
                    key_map[layer as usize].get(key_idx),
                    value_map[layer as usize].get(val_idx),
                ) else {
                    continue;
                };
                columns[key_id as usize].push((array_id as u32, value_id));
            }
        }

        AttrIndex {
            arrays,
            key_ids,
            values,
            columns,
        }
    }

    /// Number of visible arrays.
    pub(crate) fn len(&self) -> usize {
        self.arrays.len()
    }

    /// Returns the name of every visible array, in order, borrowed from the
    /// footer that holds it.
    pub(crate) fn names<'a>(
        &'a self,
        deltas: &'a [Delta<DeltaImmutable>],
    ) -> impl Iterator<Item = &'a str> + 'a {
        self.arrays.iter().map(move |&(layer, pos)| {
            deltas[layer as usize].inner.footer.arrays[pos as usize]
                .name
                .as_str()
        })
    }

    /// Returns `key` for every visible array, `None` where it is absent.
    ///
    /// The result is in the same order as [`names`](Self::names).
    pub(crate) fn column(&self, key: &str) -> Vec<Option<&AttributeValue>> {
        let mut out = vec![None; self.arrays.len()];
        let Some(&key_id) = self.key_ids.get(key) else {
            return out;
        };
        for &(array_id, value_id) in &self.columns[key_id as usize] {
            out[array_id as usize] = Some(&self.values[value_id as usize]);
        }
        out
    }
}
