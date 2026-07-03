//! # Clock Replacement Algorithm
//!
//! This module implements the Clock replacement policy, a common approximation
//! of the Least Recently Used (LRU) algorithm.

use common::BufferPoolError;

type Result<T> = std::result::Result<T, BufferPoolError>;

/// An implementation of the Clock replacement algorithm.
///
/// The `ClockReplacer` tracks which pages are currently in the buffer pool
/// and determines which page should be evicted when a new page needs to be loaded.
#[derive(Debug)]
pub struct ClockReplacer {
    size: usize,
    hand: usize,
    ref_bits: Vec<bool>,
    evictable: Vec<bool>,
}

impl ClockReplacer {
    /// Creates a new `ClockReplacer` with the specified capacity.
    pub fn new(size: usize) -> Self {
        Self {
            size,
            hand: 0,
            ref_bits: vec![false; size],
            evictable: vec![false; size],
        }
    }

    /// Finds a victim frame for eviction using the Clock algorithm.
    ///
    /// # Errors
    ///
    /// Returns [`BufferPoolError::NoEvictableFrames`] if all frames are currently pinned.
    pub fn victim(&mut self) -> Result<usize> {
        let mut searched = 0;
        let size = self.size;
        while searched < 2 * size {
            let hand = self.hand;
            if self.evictable[hand] {
                if self.ref_bits[hand] {
                    self.ref_bits[hand] = false;
                } else {
                    self.evictable[hand] = false;
                    self.ref_bits[hand] = false;
                    self.hand = (hand + 1) % size;
                    return Ok(hand);
                }
            }
            self.hand = (hand + 1) % size;
            searched += 1;
        }
        Err(BufferPoolError::NoEvictableFrames)
    }

    /// Notifies the replacer that a frame has been unpinned and is now a candidate for eviction.
    pub fn unpin(&mut self, local_id: usize) {
        self.evictable[local_id] = true;
        self.ref_bits[local_id] = true;
    }

    /// Notifies the replacer that a frame has been pinned and cannot be evicted.
    pub fn pin(&mut self, local_id: usize) {
        self.evictable[local_id] = false;
        self.ref_bits[local_id] = false;
    }
}
