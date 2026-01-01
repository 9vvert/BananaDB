下面给你一个**“4096 字节页（page）内存布局固定、可直接序列化到 bytes”**的 B+Tree 结点设计：

InnerNode：存 keys[n] + children[n+1]（child 是 B+Tree 里子结点所在的 page id）

LeafNode：存 keys[n] + rids[n]（rid 指向 table 文件里的 “第几页、第几个槽位/表项”）

全程 不用 usize（避免 32/64 位不一致导致磁盘格式不兼容）

你说你叶子存“指针”：这里用 RID (RecordId) 表示 (table_page_id, slot_index)。

下面我假设 Key 是定长 8 字节（u64）。如果你的 key 是变长（比如 VARCHAR），页内布局要换成 slotted/offset 形式（我也可以再给你那版）。

1) 基本类型与常量
pub const PAGE_SIZE: usize = 4096;

pub type PageId = u32;      // B+Tree / Table 的页号（可换 u64）
pub type SlotId = u16;      // 页内槽位（可换 u32）
pub type Key = u64;         // 假设定长 key（8 bytes）

叶子指针（RID）

为了让 RID 刚好 8 bytes（方便对齐/计算容量），用一个 padding：

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecordId {
    pub page_id: PageId, // table 文件的页号
    pub slot_id: SlotId, // 该页内第几个表项/槽
    pub _pad: u16,       // padding，让整个结构 8 bytes
}

2) LeafNode（叶子页）
Header（20 bytes）

叶子需要 prev/next 链表指针：

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct LeafHeader {
    pub node_type: u8,        // 1 = leaf
    pub _rsv0: [u8; 3],       // padding
    pub num_keys: u16,        // 当前有多少 entry
    pub _rsv1: u16,           // padding
    pub parent: PageId,       // 父节点 page id（0 表示无/根）
    pub prev_leaf: PageId,    // 叶子链表
    pub next_leaf: PageId,
}

Capacity 计算

header = 20 bytes

每条 entry：Key(8) + RecordId(8) = 16 bytes

容量：floor((4096 - 20) / 16) = 254

pub const LEAF_HEADER_SIZE: usize = core::mem::size_of::<LeafHeader>();
pub const LEAF_ENTRY_SIZE: usize =
    core::mem::size_of::<Key>() + core::mem::size_of::<RecordId>();

pub const LEAF_MAX_KEYS: usize = (PAGE_SIZE - LEAF_HEADER_SIZE) / LEAF_ENTRY_SIZE; // 254

结构体

为了“页内固定布局、可直接当 bytes 写入”，用定长数组：

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct LeafNode {
    pub hdr: LeafHeader,
    pub keys: [Key; LEAF_MAX_KEYS],
    pub rids: [RecordId; LEAF_MAX_KEYS],
}

3) InnerNode（内部页）

内部页存：children[n+1] + keys[n]（典型 B+Tree 结构）

Header（12 bytes）
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct InnerHeader {
    pub node_type: u8,     // 0 = inner
    pub level: u8,         // 0 表示叶子层之上那层？(你可自定义)
    pub _rsv0: [u8; 2],    // padding
    pub num_keys: u16,     // keys 数量 = n
    pub _rsv1: u16,        // padding
    pub parent: PageId,
}

Capacity 计算（关键）

Inner 页空间占用：

header: 12

keys: n * 8

children: (n+1) * 4 （因为 PageId=u32）

总和：12 + 8n + 4(n+1) = 16 + 12n <= 4096
所以 n <= floor((4096-16)/12) = 340

pub const INNER_HEADER_SIZE: usize = core::mem::size_of::<InnerHeader>();

// 解不等式：INNER_HEADER_SIZE + n*size(Key) + (n+1)*size(PageId) <= PAGE_SIZE
pub const INNER_MAX_KEYS: usize = (PAGE_SIZE - INNER_HEADER_SIZE - core::mem::size_of::<PageId>())
    / (core::mem::size_of::<Key>() + core::mem::size_of::<PageId>()); // 340

pub const INNER_MAX_CHILDREN: usize = INNER_MAX_KEYS + 1; // 341

结构体
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct InnerNode {
    pub hdr: InnerHeader,
    pub children: [PageId; INNER_MAX_CHILDREN], // n+1
    pub keys: [Key; INNER_MAX_KEYS],            // n
}

4) 设计约定（你实现算法时会用到）
Leaf 约定

hdr.num_keys = m 表示 keys[0..m] 和 rids[0..m] 有效

叶子链表用于范围扫描：prev_leaf / next_leaf

Inner 约定

hdr.num_keys = n

keys[0..n] 有效

children[0..n+1] 有效

查找逻辑：

找到第一个 key[i] > target，走 children[i]

如果全都 <= target，走 children[n]

5) 重要提醒：PageId 用 u32 vs u64

我这里用 u32 是为了提高扇出（fanout），性能通常更好。代价是：

table/btree 文件最多约 2^32 个 page（乘以 4096B 约 16TB 级别），对很多场景够用
如果你必须更大，把 PageId=u64，容量会下降（inner 扇出变小，leaf entry size 也可能变大）。
