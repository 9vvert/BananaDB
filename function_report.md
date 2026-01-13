# Function Report for `src/index`

## `src/index/mod.rs` – `BPlusTree<'a, const PAGE_NUM: usize>`
- `new(buf, table_name, col_idx, col_type) -> Self`: Configures a B+ tree wrapper with buffer cache reference and column metadata. Precomputes fixed key width from `ColumnType` and records `col_idx` as `extra_info` for page identity.
- `header_page(&mut self) -> HeaderPage<'_>`: Retrieves page 0 for this table/index from the cache as a header view. Lifetime ties to `self` to keep borrow valid while the page is in use.
- `ensure_header(&mut self)`: Lazily initializes the header if magic is missing, using stored column index. Safe to call repeatedly; idempotent.
- `root_page_id(&mut self) -> u32`: Ensures header exists, then reads the root page pointer from it.
- `set_root_page(&mut self, root: u32)`: Ensures header exists, then writes a new root page pointer to the header (marking dirty).
- `alloc_page(&mut self) -> u32`: Ensures header exists and bumps the monotonically increasing `next_page_id`, returning the id allocated for a new index page.
- `compare_key(col_type, a, b) -> Ordering`: Static helper to compare serialized keys respecting type. `INT` compares little-endian i32s; `CHAR` trims trailing null padding before UTF-8 string comparison.
- `encode_key(&self, v) -> Result<Vec<u8>, String>`: Serializes a `ColumnValue` into fixed-length bytes for the index key. Pads/truncates CHAR to declared length; errors on column-type mismatch.
- `leftmost_leaf(&mut self, root: u32) -> u32`: Descends via `child0` pointers from `root` until a leaf (or broken link) is found. Returns the id of the leftmost leaf reachable.
- `find_leaf(&mut self, key, root) -> (u32, Vec<u32>)`: Traverses internal nodes using separator-key comparisons to locate the target leaf for `key`. Returns the leaf page id plus the traversal path (root to leaf) for later upward propagation.
- `insert_into_leaf(&mut self, page_id, key, rid) -> Result<Option<(Vec<u8>, u32)>, String>`: Inserts `(key, RecordId)` into a leaf in sorted order, shifting entries as needed and updating counts. On overflow, splits roughly in half, relinks `next_leaf`, initializes a new leaf page, and returns the new page id plus its first key as separator; otherwise returns `None`.
- `insert_into_internal(&mut self, page_id, left_child, separator, right_child) -> Result<Option<(Vec<u8>, u32)>, String>`: Inserts `(separator, right_child)` into an internal node directly to the right of `left_child`. Validates node type, shifts to make space, and updates key count. On overflow, splits around the middle key (promoted upwards), seeds a new internal page with the right half and first-child pointer, and returns `(promote_key, new_page_id)`; otherwise returns `None`.
- `insert(&mut self, key_val, rid) -> Result<(), String>`: Public insert entry point. Encodes key, ensures header/root; if tree empty, allocates and seeds first leaf then sets root. Otherwise inserts into leaf, handling cascaded splits up the recorded path and creating a new root if the old root splits.
- `search_leaf_rids(&mut self, lower: &Bound, upper: &Bound, root: u32) -> Result<Vec<RecordId>, String>`: Performs range scan within leaves. Encodes bounds, picks starting leaf (lower-bound leaf or leftmost), then walks linked leaves collecting `RecordId`s while respecting inclusive/exclusive bound checks and stopping when upper bound exceeded.
- `search_range(&mut self, lower: Bound, upper: Bound) -> Result<Vec<RecordId>, String>`: Public range search wrapper. Ensures header, returns empty for an uninitialized tree, otherwise delegates to `search_leaf_rids`.
- `delete(&mut self, key_val, rid) -> Result<(), String>`: Deletes a specific `(key, RecordId)` from the located leaf. Scans for exact match, shifts entries left to fill the gap, decrements key count, and marks dirty. Does not rebalance/merge on underflow (TODO).

## `src/index/node.rs` – Header and Node helpers
### `HeaderPage<'a>`
- `new(page) -> Self`: Wraps a mutable `Page` as a header accessor.
- `init_if_needed(col_idx)`: Detects missing/invalid magic; if absent, zeroes the page, writes magic, sets root to `INVALID_PAGE`, next page id to 1, stores column index, and marks page dirty.
- `root(&self) -> u32`: Reads current root page id from bytes 4..8.
- `set_root(&mut self, page_id)`: Writes root pointer to bytes 4..8 and marks dirty.
- `next_page_id(&self) -> u32`: Reads allocation counter from bytes 8..12.
- `set_next_page_id(&mut self, val)`: Writes allocation counter and marks dirty.
- `col_idx(&self) -> usize`: Reads indexed column index from bytes 12..16.
- `set_col_idx(&mut self, val)`: Writes column index and marks dirty.
- `alloc_page(&mut self) -> u32`: Returns current `next_page_id` then increments it via `set_next_page_id`, providing monotonically increasing page ids.

### `IndexNodePage<'a>`
- `new(data, key_size) -> Self`: Creates a typed view over raw node page bytes with known key width.
- `node_type(&self) -> NodeType`: Reads node kind tag from bytes 0..4 (`0` leaf, `1` internal), panicking on unknown value.
- `set_node_type(&mut self, ty)`: Writes node type tag.
- `key_count(&self) -> usize`: Reads the stored key count from bytes 4..8.
- `set_key_count(&mut self, cnt)`: Writes key count back to bytes 4..8.
- `next_leaf(&self) -> u32`: Reads leaf-level next pointer from bytes 8..12 (meaningful only for leaves).
- `set_next_leaf(&mut self, next)`: Writes next-leaf pointer.
- `leaf_entry_size(&self) -> usize`: Computes per-leaf-entry size = `key_size + 4` (record id).
- `internal_entry_size(&self) -> usize`: Computes per-internal-entry size = `key_size + 4` (right child id).
- `leaf_capacity(&self) -> usize`: Maximum leaf entries fitting after header.
- `internal_capacity(&self) -> usize`: Maximum internal keys given header plus leading child0 slot.
- `leaf_entry_offset(&self, index) -> usize`: Byte offset of the `index`th leaf entry after header.
- `internal_first_child_offset(&self) -> usize`: Byte offset of the leftmost child pointer (immediately after header).
- `internal_entry_offset(&self, index) -> usize`: Byte offset of the `index`th `(key, child)` pair that follows child0.
- `leaf_key_bytes(&self, idx) -> &[u8]`: Slice over key bytes of leaf entry `idx`.
- `leaf_record_id(&self, idx) -> RecordId`: Reads and deserializes the record id stored after the key at entry `idx`.
- `write_leaf_entry(&mut self, idx, key, rid)`: Writes key bytes and record id at entry slot `idx`.
- `shift_leaf_entries(&mut self, start, end)`: Shifts leaf entries in `[start, end)` one slot to the right to open a hole.
- `shift_leaf_entries_left(&mut self, start, end)`: Shifts leaf entries in `[start, end)` one slot left to close a gap.
- `internal_child_at(&self, idx) -> u32`: Returns the child pointer at position `idx` (child0 for 0, otherwise child following key `idx-1`); asserts `idx <= key_count`.
- `set_internal_child0(&mut self, child)`: Writes the leftmost child pointer slot.
- `internal_key_bytes(&self, idx) -> &[u8]`: Slice over the separator key at index `idx`.
- `write_internal_entry(&mut self, idx, key, child)`: Writes separator key and right-child pointer at slot `idx`.
- `shift_internal_entries(&mut self, start, end)`: Shifts internal entries in `[start, end)` one slot right to create space.
- `shift_internal_entries_left(&mut self, start, end)`: Shifts internal entries in `[start, end)` one slot left to compact after deletion.
