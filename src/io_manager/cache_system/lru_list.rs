use std::error::Error;

// keypoint:
// lru_list is only used to "judge" which page to drop/allocate.
// read/write page, update map, etc. are done in cache_system
//
// cache_system map : table_id+page_id => lru_list vector index

// 规定LruNode所在Vec中的索引和cache page索引相同
struct LruNode {
    prev: Option<usize>,
    next: Option<usize>,
}

pub struct LruList<const PAGE_NUM: usize> {
    list: Vec<LruNode>,
    list_len: usize,
    head_ptr: Option<usize>,
}

impl LruNode {
    fn new(prev_ptr: Option<usize>, next_ptr: Option<usize>) -> Self {
        LruNode {
            prev: prev_ptr,
            next: next_ptr,
        }
    }
}

impl<const PAGE_NUM: usize> LruList<PAGE_NUM> {
    pub fn new() -> Self {
        LruList {
            list: vec![],
            list_len: 0,
            head_ptr: None,
        }
    }
    pub fn have_free_page(&self) -> bool {
        return self.list_len < PAGE_NUM;
    }
    // 返回lru_id (供上层添加到map记录)
    pub fn new_page(&mut self) -> Result<(), Box<dyn Error>> {
        // new list
        if self.list_len == 0 {
            self.head_ptr = Some(0);
            self.list.push(LruNode::new(Some(0), Some(0)));
            self.list_len += 1;
            Ok(())
        }
        // allocate new
        else if self.list_len < PAGE_NUM {
            let lru_id = self.list_len;
            self.list.push(LruNode::new(Some(lru_id), Some(lru_id)));
            let head_prev_lru_id = self.list[self.head_ptr.unwrap()].prev.unwrap();
            self.list[head_prev_lru_id].next = Some(lru_id);
            self.list[lru_id].prev = Some(head_prev_lru_id);
            self.list[self.head_ptr.unwrap()].prev = Some(lru_id);
            self.list[lru_id].next = Some(self.head_ptr.unwrap());
            // update head ptr
            self.head_ptr = Some(lru_id);

            self.list_len += 1;

            Ok(())
        }
        // full (不能直接在这里完成所有的操作，因为需要通知上层完成IO)
        else {
            Err(format!("Cannot add new node. Current list length:{}", self.list_len).into())
        }
    }
    // 实际使用的时候，删除任意节点的操作可以继续封装
    pub fn lift_page(&mut self, lru_id: usize) -> Result<(), Box<dyn Error>> {
        if lru_id >= self.list_len {
            return Err(format!(
                "Invalid LRU list index {}, current list length:{}",
                lru_id, self.list_len
            )
            .into());
        } else {
            if lru_id != self.head_ptr.unwrap() {
                let obj_node = &self.list[lru_id];
                let obj_prev_lru_id = obj_node.prev.unwrap();
                let obj_next_lru_id = obj_node.next.unwrap();
                self.list[obj_prev_lru_id].next = Some(obj_next_lru_id);
                self.list[obj_next_lru_id].prev = Some(obj_prev_lru_id);

                let head_prev_lru_id = self.list[self.head_ptr.unwrap()].prev.unwrap();
                self.list[head_prev_lru_id].next = Some(lru_id);
                self.list[lru_id].prev = Some(head_prev_lru_id);
                self.list[self.head_ptr.unwrap()].prev = Some(lru_id);
                self.list[lru_id].next = Some(self.head_ptr.unwrap());

                self.head_ptr = Some(lru_id);
            }
            Ok(())
        }
    }
    // return the cache_id of a page
    pub fn get_drop_page(&mut self) -> Result<usize, Box<dyn Error>> {
        match self.head_ptr {
            Some(head_id) => {
                Ok(self.list[head_id].prev.unwrap())
                // 返回的值让上层进行page读写
                // 这里应该假设其已经被废弃，直接从list中drop
            }
            None => Err("Cannot drop from empty list".into()),
        }
    }
    //
}
