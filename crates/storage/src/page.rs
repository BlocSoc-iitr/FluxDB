pub const PAGE_SIZE: usize = 4096;
pub type PageId = u64; 
pub type Lsn = u64; // 8 bytes 
pub enum BPlusTreePage <K> {
    Internal(BPlusTreeInternalPage <K>),
    Leaf(BPlusTreeLeafPage),
}
#[derive(Debug,Clone)]
pub enum BPlusTreePageType {
    LeafPage,
    InternalPage,
}


#[derive(Debug, Clone)]
pub struct InternalKV<K> {
    pub key: K,
    pub page_id: PageId, // this page id links to the downward node
}

/*
 * Internal Node Structure:
 *
 * ---------------------------------------------------------------------------------
 * | Header | P1 | K1 | P2 | K2 | P3 | ... | Kn | Pn+1 |          Free Space       |
 * ---------------------------------------------------------------------------------
 *
 * Header contains: page_type, current_size, max_size, next_page_id
 * Elements: Vector of InternalKV (Key, PageId). The first key is conceptually empty,
 * and its page_id field is used as the leftmost child pointer (P1).
 */

#[derive(Debug, Clone)]
pub struct BPlusTreeInternalPageHeader {
    pub page_type: BPlusTreePageType,
    pub current_size: u32,
    pub max_size: u32,       
    pub next_page_id: PageId, // this page id links to the right sibling horizontally
}
// simple vec of key-value pair can be used and we don't need slotted pages architecture since internal nodes are of fixed size. 
#[derive(Debug, Clone)]
pub struct BPlusTreeInternalPage<K> {
    pub header: BPlusTreeInternalPageHeader,

    // This is conceptually an array where Index 0 has an empty Key, 
    pub elements: Vec<InternalKV<K>>,
    
}

// array of key-value pair is a bad choice since rows can be of variable size. 
#[derive(Debug, Clone)]
pub struct BPlusTreeLeafPage {
    pub header: BPlusTreeLeafPageHeader,
    // 整个页原始数据
    pub data: [u8; PAGE_SIZE],
}

#[derive(Debug, Clone)]
/**
 * Slotted page format:
 * ```text
 *  ---------------------------------------------------------
 *  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
 *  ---------------------------------------------------------
 *                                ^
 *                                free space pointer
 * ```
 *
 * Header format (size in bytes):
 * ```text
 *  -----------------------------------------------------------------------------------------
 *  | PageType (1) | LSN (8) | NextPageId (8) | NumTuples(2) | NumDeletedTuples(2) | free_Space_pointer(2) |
 *  -----------------------------------------------------------------------------------------
 *  ----------------------------------------------------------------
 *  | Tuple_1 offset+size + TupleMeta | Tuple_2 offset+size + TupleMeta | ... |
 *  ----------------------------------------------------------------
 * ```
 */
pub struct BPlusTreeLeafPageHeader{
    pub page_type: BPlusTreePageType,
    pub next_page_id: PageId,
    pub num_tuples: u16,
    pub num_deleted_tuples: u16,
    pub tuple_infos: Vec<TupleInfo>,
    pub free_space_pointer: u16, // since we will keep keys sorted its not necessary that offset of last key will be the true offset
    pub lsn: Lsn,
}


#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TupleInfo {
    pub offset: u16,
    pub size: u16,
    pub meta: TupleMeta,
}

// Fast-access metadata for MVCC (Multi-Version Concurrency Control), tracking transaction IDs 
// uncomment once we define transactionId, CommandId
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleMeta {
    // pub insert_txn_id: TransactionId,
    // pub insert_cid: CommandId,
    // pub delete_txn_id: TransactionId,
    // pub delete_cid: CommandId,
    // pub is_deleted: bool,
    // pub next_version: Option<RecordId>,
    // pub prev_version: Option<RecordId>,
}

impl BPlusTreeLeafPage {
    pub fn new(next_page_id: PageId) -> Self {
        Self {
            header: BPlusTreeLeafPageHeader {
            page_type: BPlusTreePageType::LeafPage,
            next_page_id,
            num_tuples: 0,
            num_deleted_tuples: 0,
            tuple_infos: Vec::new(),
            lsn: 0,
            free_space_pointer: PAGE_SIZE as u16,
        },
        data: [0;PAGE_SIZE],
        }
    }
    // calculate offset when length is known
    pub fn next_tuple_offset_with_len(&self, tuple_len: usize) -> Result<usize, &'static str> {
        // 1. Check if the tuple is larger than all remaining space
        if (self.header.free_space_pointer as usize) < tuple_len {
            return Err("Not enough free space to store tuple");
        }

        // 2. Provisional O(1) offset calculation
        let tuple_offset = (self.header.free_space_pointer as usize) - tuple_len;

        // 3. Prevent collision with the header (which grows forward)
        let base_header_size = 23; 
        let current_slots_size = self.header.tuple_infos.len() * size_of::<TupleInfo>();
        let new_slot_size = size_of::<TupleInfo>();
        
        let min_tuple_offset = base_header_size + current_slots_size + new_slot_size;

        if tuple_offset < min_tuple_offset {
            return Err("Not enough free space: Header collided with Data");
        }

        Ok(tuple_offset)
    }

    pub fn insert_tuple_bytes(
        &mut self,
        meta: &TupleMeta,
        tuple_bytes: &[u8],
    ) -> Result<u16, &'static str> {
        // Get the offset for the next tuple insertion.
        let tuple_offset = self.next_tuple_offset_with_len(tuple_bytes.len())?;
        let tuple_id = self.header.num_tuples;
        debug_assert!(tuple_bytes.len() < u16::MAX as usize);

        // Store tuple information including offset, length, and metadata.
        self.header.tuple_infos.push(TupleInfo {
            offset: tuple_offset as u16,
            size: tuple_bytes.len() as u16,
            meta: *meta,
        });

        // only check
        assert_eq!(tuple_id, self.header.tuple_infos.len() as u16 - 1);

        self.header.num_tuples += 1;

        // if meta.is_deleted {
        //     self.header.num_deleted_tuples += 1;
        // }

        // Copy the tuple's data into the appropriate position within the page's data buffer.
        self.data[tuple_offset..tuple_offset + tuple_bytes.len()].copy_from_slice(&tuple_bytes);
        Ok(tuple_id)
    }

    pub fn tuple(&self, slot_num: u16) -> Result<(TupleMeta, &[u8]), &'static str> {
        if slot_num >= self.header.num_tuples {
            return Err(
                "tuple_id out of range",
            );
        }

        let offset = self.header.tuple_infos[slot_num as usize].offset;
        let size = self.header.tuple_infos[slot_num as usize].size;
        let meta = self.header.tuple_infos[slot_num as usize].meta;

        let tuple_bytes = &self.data[offset as usize..(offset + size) as usize]; 

        Ok((meta, tuple_bytes))
    }

    pub fn set_lsn(&mut self, lsn: Lsn) {
        self.header.lsn = lsn;
    }

    pub fn lsn(&self) -> Lsn {
        self.header.lsn
    }
}

