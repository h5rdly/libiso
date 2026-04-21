use crate::lzms_arrays::{
    LZMS_LENGTH_SLOT_BASE, LZMS_EXTRA_LENGTH_BITS, LZMS_OFFSET_SLOT_BASE, LZMS_EXTRA_OFFSET_BITS,
};


// Probability Context Sizes (2^N where N is history bits)
const NUM_MAIN_PROBS: usize = 16;  // 4 bits history (Literal vs Match)
const NUM_MATCH_PROBS: usize = 32; // 5 bits history (LZ vs Delta)
const NUM_LZ_PROBS: usize = 64;    // 6 bits history (Explicit vs Repeat)
const NUM_DELTA_PROBS: usize = 64; 
const NUM_REP_PROBS: usize = 64;   // Used for all rep-match decisions

// Huffman Table Configurations
const NUM_LITERAL_SYMS: usize = 256;
const NUM_LENGTH_SYMS: usize = 54;
const MAX_NUM_OFFSET_SYMS: usize = 799;
const NUM_DELTA_POWER_SYMS: usize = 8;

const LZMS_X86_MAX_TRANSLATION_OFFSET: i32 = 1073741823; // 0x3FFFFFFF
const LZMS_X86_ID_WINDOW_SIZE: i32 = 65535;              // 0xFFFF



pub struct LzmsDecompressor<'a> {
    range_dec: ForwardRangeDecoder<'a>,
    bitstream: BackwardBitstream<'a>,

    // --- Probability Models & Context States ---
    
    main_probs: [ProbabilityEntry; NUM_MAIN_PROBS],
    main_state: u32,

    match_probs: [ProbabilityEntry; NUM_MATCH_PROBS],
    match_state: u32,

    lz_probs: [ProbabilityEntry; NUM_LZ_PROBS],
    lz_state: u32,

    // 3 decisions for LZ Rep matches (is_rep0, if not is_rep1, if not is_rep2)
    lz_rep_probs: [[ProbabilityEntry; NUM_REP_PROBS]; 3],
    lz_rep_states: [u32; 3],

    delta_probs: [ProbabilityEntry; NUM_DELTA_PROBS],
    delta_state: u32,

    delta_rep_probs: [[ProbabilityEntry; NUM_REP_PROBS]; 3],
    delta_rep_states: [u32; 3],

    // (NUM_SYMS, TABLE_BITS, TABLE_SIZE)
    // TABLE_SIZE must be >= (1 << TABLE_BITS) + (NUM_SYMS * 2)
    literal_code: LzmsHuffmanCode<NUM_LITERAL_SYMS, 10, 1536>,
    length_code: LzmsHuffmanCode<NUM_LENGTH_SYMS, 9, 620>,
    lz_offset_code: LzmsHuffmanCode<MAX_NUM_OFFSET_SYMS, 11, 3646>,
    delta_offset_code: LzmsHuffmanCode<MAX_NUM_OFFSET_SYMS, 11, 3646>,
    delta_power_code: LzmsHuffmanCode<NUM_DELTA_POWER_SYMS, 7, 144>,
}

impl<'a> LzmsDecompressor<'a> {

    pub fn new(data: &'a [u8]) -> Option<Self> {
        let range_dec = ForwardRangeDecoder::new(data)?;
        let bitstream = BackwardBitstream::new(data);

        Some(Self {
            range_dec,
            bitstream,

            main_probs: [ProbabilityEntry::new(); NUM_MAIN_PROBS],
            main_state: 0,

            match_probs: [ProbabilityEntry::new(); NUM_MATCH_PROBS],
            match_state: 0,

            lz_probs: [ProbabilityEntry::new(); NUM_LZ_PROBS],
            lz_state: 0,

            lz_rep_probs: [[ProbabilityEntry::new(); NUM_REP_PROBS]; 3],
            lz_rep_states: [0; 3],

            delta_probs: [ProbabilityEntry::new(); NUM_DELTA_PROBS],
            delta_state: 0,

            delta_rep_probs: [[ProbabilityEntry::new(); NUM_REP_PROBS]; 3],
            delta_rep_states: [0; 3],

            // Initialization with official WIM rebuild frequencies
            literal_code: LzmsHuffmanCode::new(1024),
            length_code: LzmsHuffmanCode::new(512),
            lz_offset_code: LzmsHuffmanCode::new(1024),
            delta_offset_code: LzmsHuffmanCode::new(1024),
            delta_power_code: LzmsHuffmanCode::new(512),
        })
    }


    #[inline(always)]
    fn decode_delta_power(&mut self) -> u32 {
        self.delta_power_code.decode_symbol(&mut self.bitstream) as u32
    }

    #[inline(always)]
    fn decode_delta_offset(&mut self) -> u32 {
        let slot = self.delta_offset_code.decode_symbol(&mut self.bitstream) as usize;
        let mut offset = LZMS_OFFSET_SLOT_BASE[slot];
        let num_extra_bits = LZMS_EXTRA_OFFSET_BITS[slot] as u32;
        
        if num_extra_bits > 0 {
            offset += self.bitstream.read_bits(num_extra_bits);
        }
        
        offset
    }

    pub fn decompress_block(&mut self, out_buf: &mut [u8]) -> Result<(), &'static str> {

        let mut out_pos = 0;
        let out_end = out_buf.len();

        // LRU Queues initialized to 1, 2, 3, 4. 
        // We need 4 slots because of the delayed update mechanic.
        let mut recent_lz_offsets = [1u32, 2, 3, 4];
        let mut recent_delta_pairs = [1u64, 2, 3, 4];

        // 0 = literal, 1 = LZ match, 2 = delta match
        let mut prev_item_type = 0; 

        while out_pos < out_end {
            // 1. Literal vs Match
            let bit = self.range_dec.decode_bit(
                &mut self.main_probs[self.main_state as usize],
                &mut self.main_state,
                16,
            );

            if bit == 0 {
                // Literal
                let sym = self.literal_code.decode_symbol(&mut self.bitstream);
                out_buf[out_pos] = sym as u8;
                out_pos += 1;
                prev_item_type = 0;
                continue;
            } 
            
            // LZ Match vs Delta Match
            let is_delta = self.range_dec.decode_bit(
                &mut self.match_probs[self.match_state as usize],
                &mut self.match_state,
                32,
            );

            if is_delta == 0 {
                // LZ MATCH 
                let is_rep = self.range_dec.decode_bit(
                    &mut self.lz_probs[self.lz_state as usize],
                    &mut self.lz_state,
                    64,
                );

                let offset;
                if is_rep == 0 {
                    // Explicit Offset
                    offset = self.decode_lz_offset();
                    recent_lz_offsets[3] = recent_lz_offsets[2];
                    recent_lz_offsets[2] = recent_lz_offsets[1];
                    recent_lz_offsets[1] = recent_lz_offsets[0];
                } else {
                    // Repeat Offset
                    let mut rep_idx = 0;
                    if self.range_dec.decode_bit(&mut self.lz_rep_probs[0][self.lz_rep_states[0] as usize], &mut self.lz_rep_states[0], 64) != 0 {
                        rep_idx = 1;
                        if self.range_dec.decode_bit(&mut self.lz_rep_probs[1][self.lz_rep_states[1] as usize], &mut self.lz_rep_states[1], 64) != 0 {
                            rep_idx = 2;
                        }
                    }

                    // Apply the delayed update shift
                    let delay = prev_item_type & 1;
                    let target_idx = rep_idx + delay;
                    offset = recent_lz_offsets[target_idx];
                    
                    // Shift elements down to make room at the front
                    for i in (1..=target_idx).rev() {
                        recent_lz_offsets[i] = recent_lz_offsets[i - 1];
                    }
                }
                
                recent_lz_offsets[0] = offset;
                prev_item_type = 1;

                let length = self.decode_length() as usize;

                // Execute the LZ Copy
                if offset as usize > out_pos { return Err("LZ offset out of bounds"); }
                if out_pos + length > out_end { return Err("LZ copy exceeds buffer"); }
                
                // We cannot automatically use copy_within because LZ77 dictates that 
                // source and  destination regions can overlap (e.g. for repeating patterns).
                let offset_usize = offset as usize;
                if offset_usize >= length {
                    // No overlap, use highly optimized memmove
                    out_buf.copy_within((out_pos - offset_usize)..(out_pos - offset_usize + length), out_pos);
                    out_pos += length;
                } else {
                    // Overlapping repeat pattern (e.g., AAAAAA)
                    for _ in 0..length {
                        out_buf[out_pos] = out_buf[out_pos - offset_usize];
                        out_pos += 1;
                    }
                }

            } else {
                // DELTA MATCH 
                let is_rep = self.range_dec.decode_bit(
                    &mut self.delta_probs[self.delta_state as usize],
                    &mut self.delta_state,
                    64,
                );

                let pair;
                let power;
                let raw_offset;

                if is_rep == 0 {
                    // Explicit Delta
                    power = self.decode_delta_power();
                    raw_offset = self.decode_delta_offset();
                    let raw = self.decode_delta_offset();
                    pair = ((power as u64) << 32) | (raw as u64);
                    
                    recent_delta_pairs[3] = recent_delta_pairs[2];
                    recent_delta_pairs[2] = recent_delta_pairs[1];
                    recent_delta_pairs[1] = recent_delta_pairs[0];
                } else {
                    // Repeat Delta
                    let mut rep_idx = 0;
                    if self.range_dec.decode_bit(&mut self.delta_rep_probs[0][self.delta_rep_states[0] as usize], &mut self.delta_rep_states[0], 64) != 0 {
                        rep_idx = 1;
                        if self.range_dec.decode_bit(&mut self.delta_rep_probs[1][self.delta_rep_states[1] as usize], &mut self.delta_rep_states[1], 64) != 0 {
                            rep_idx = 2;
                        }
                    }

                    // Apply the delayed update shift (Note: delay condition is slightly different for Deltas)
                    let delay = prev_item_type >> 1;
                    let target_idx = rep_idx + delay;
                    pair = recent_delta_pairs[target_idx];
                    
                    for i in (1..=target_idx).rev() {
                        recent_delta_pairs[i] = recent_delta_pairs[i - 1];
                    }
                    
                    power = (pair >> 32) as u32;
                    raw_offset = pair as u32;
                }
                
                recent_delta_pairs[0] = pair;
                prev_item_type = 2;

                let length = self.decode_length() as usize;

                let span = 1usize << power;
                let offset = (raw_offset as usize) << power;

                // Security checks for the Delta Copy
                if offset + span > out_pos { return Err("Delta offset out of bounds"); }
                if out_pos + length > out_end { return Err("Delta copy exceeds buffer"); }

                // Execute the Delta Copy
                // D[i] = A[i] + B[i-span] - C[i-offset-span]
                for _ in 0..length {
                    let a = out_buf[out_pos - offset];
                    let b = out_buf[out_pos - span];
                    let c = out_buf[out_pos - offset - span];
                    
                    out_buf[out_pos] = a.wrapping_add(b).wrapping_sub(c);
                    out_pos += 1;
                }
            }
        }
        
        Ok(())
    }


    #[inline(always)]
    fn decode_length(&mut self) -> u32 {
        // 1. Decode the Huffman symbol to get the slot
        let slot = self.length_code.decode_symbol(&mut self.bitstream) as usize;
        
        // 2. Look up the base and the extra bits required
        let mut length = LZMS_LENGTH_SLOT_BASE[slot];
        let num_extra_bits = LZMS_EXTRA_LENGTH_BITS[slot] as u32;
        
        // 3. If extra bits are needed, read them raw from the bitstream and add
        if num_extra_bits > 0 {
            length += self.bitstream.read_bits(num_extra_bits);
        }
        
        length
    }

    #[inline(always)]
    fn decode_lz_offset(&mut self) -> u32 {
        let slot = self.lz_offset_code.decode_symbol(&mut self.bitstream) as usize;
        let mut offset = LZMS_OFFSET_SLOT_BASE[slot];
        let num_extra_bits = LZMS_EXTRA_OFFSET_BITS[slot] as u32;
        
        if num_extra_bits > 0 {
            offset += self.bitstream.read_bits(num_extra_bits);
        }
        
        offset
    }
}




// -- Probability Model

#[derive(Clone, Copy)]
pub struct ProbabilityEntry {
    recent_bits: u64,
    num_recent_zero_bits: u32,
}

impl ProbabilityEntry {
    pub const fn new() -> Self {
        Self {
            // LZMS initializes with 48 zeroes and 16 ones.
            recent_bits: 0x0000000055555555,
            num_recent_zero_bits: 48,
        }
    }

    // Returns the probability out of 64 (denominator is fixed in LZMS)
    #[inline(always)]
    pub fn get_probability(&self) -> u32 {
        // LZMS forbids 0% and 100% probabilities.
        match self.num_recent_zero_bits {
            0 => 1,
            64 => 63,
            n => n,
        }
    }

    // Shifts the window and updates the zero count
    #[inline(always)]
    pub fn update(&mut self, bit: u32) {

        let oldest_bit = (self.recent_bits >> 63) as u32;
        self.recent_bits = (self.recent_bits << 1) | (bit as u64);
        if oldest_bit == 0 {
            self.num_recent_zero_bits -= 1;
        }
        if bit == 0 {
            self.num_recent_zero_bits += 1;
        }
    }
}



// -- Forward range decoder 

pub struct ForwardRangeDecoder<'a> {
    data: &'a [u8],
    pos: usize,
    range: u32,
    code: u32,
}

impl<'a> ForwardRangeDecoder<'a> {
    pub fn new(data: &'a [u8]) -> Option<Self> {
        // We need at least 4 bytes to initialize the 32-bit code.
        if data.len() < 4 { return None; }
        
        let code_high = u16::from_le_bytes([data[0], data[1]]) as u32;
        let code_low = u16::from_le_bytes([data[2], data[3]]) as u32;
        
        Some(Self {
            data,
            pos: 4,
            range: 0xFFFFFFFF,
            code: (code_high << 16) | code_low,
        })
    }

    // Decode a single bit based on the given probability entry.
    // Update the state index (context) along the way.
    #[inline(always)]
    pub fn decode_bit(
        &mut self, 
        prob_entry: &mut ProbabilityEntry, 
        state: &mut u32, 
        num_states: u32
    ) -> u32 {
        let prob = prob_entry.get_probability();
        
        // Update state early for the next bit context (shifted left)
        *state = (*state << 1) & (num_states - 1);
        
        // Normalize range if it gets too small
        if (self.range & 0xFFFF0000) == 0 {
            self.range <<= 16;
            self.code <<= 16;
            
            // Safe Rust slice read
            if self.pos + 1 < self.data.len() {
                let word = u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]) as u32;
                self.code |= word;
                self.pos += 2;
            }
        }
        
        // LZMS shifts by 6 because probabilities are fractions of 64 (2^6).
        let bound = (self.range >> 6) * prob;
        
        if self.code < bound {
            // Decoded a 0
            self.range = bound;
            prob_entry.update(0);
            0
        } else {
            // Decoded a 1
            self.range -= bound;
            self.code -= bound;
            prob_entry.update(1);
            *state |= 1;
            1
        }
    }
}


// -- Backward bitstream 

pub struct BackwardBitstream<'a> {
    data: &'a [u8],
    pos: usize,
    bitbuf: u64,
    bitsleft: u32,
}

impl<'a> BackwardBitstream<'a> {
    // Initialize the bitstream at the very end of the data buffer.
    pub fn new(data: &'a [u8]) -> Self {
        // LZMS blocks are always aligned to 16-bit boundaries.
        debug_assert!(data.len() % 2 == 0, "LZMS data must be even length");
        Self {
            data,
            pos: data.len(),
            bitbuf: 0,
            bitsleft: 0,
        }
    }

    // Pulls 16-bit chunks from the buffer into the 64-bit accumulator 
    // until we have at least `num_bits` available.
    #[inline(always)]
    pub fn ensure_bits(&mut self, num_bits: u32) {
        // As long as we need bits and haven't hit the start of the buffer...
        while self.bitsleft < num_bits && self.pos >= 2 {
            self.pos -= 2;
            
            // Read a 16-bit little-endian word
            let word = u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]) as u64;
            
            // Shift the word into the highest available position in the accumulator
            // Since we shift bits out to the left (MSB), new bits come in from the right.
            let shift = 48 - self.bitsleft; 
            self.bitbuf |= word << shift;
            self.bitsleft += 16;
        }
    }

    // Look at the next `num_bits` without advancing the bitstream
    #[inline(always)]
    pub fn peek(&mut self, num_bits: u32) -> u32 {
        self.ensure_bits(num_bits);
        // The valid bits are left-aligned at the top of the u64
        // We shift them all the way to the right to return them as a u32
        (self.bitbuf >> (64 - num_bits)) as u32
    }

    // Advance the bitstream by `num_bits`
    #[inline(always)]
    pub fn consume(&mut self, num_bits: u32) {
        // Shift left to discard the consumed bits.
        self.bitbuf <<= num_bits;
        self.bitsleft -= num_bits;
    }

    ///Peek and consume in one motion.
    #[inline(always)]
    pub fn read_bits(&mut self, num_bits: u32) -> u32 {
        let bits = self.peek(num_bits);
        self.consume(num_bits);
        bits
    }
}


// -- Huffman Code

pub struct LzmsHuffmanCode<const NUM_SYMS: usize, const TABLE_BITS: usize, const TABLE_SIZE: usize> {
    // O(1) fast-path lookup table.
    decode_table: [u16; TABLE_SIZE],
    
    freqs: [u32; NUM_SYMS],
    lens: [u8; NUM_SYMS],
    codewords: [u32; NUM_SYMS],
    
    syms_until_rebuild: u32,
    rebuild_freq: u32,
}


impl<const NUM_SYMS: usize, const TABLE_BITS: usize, const TABLE_SIZE: usize> 
    LzmsHuffmanCode<NUM_SYMS, TABLE_BITS, TABLE_SIZE> {
    
    pub fn new(rebuild_freq: u32) -> Self {
        assert!(
            TABLE_SIZE >= (1 << TABLE_BITS) + (NUM_SYMS * 2), 
            "TABLE_SIZE must be large enough for the fast-path table AND the overflow tree"
        );
       
        let mut code = Self {
            decode_table: [0; TABLE_SIZE],
            freqs: [1; NUM_SYMS],
            lens: [0; NUM_SYMS],
            codewords: [0; NUM_SYMS],
            syms_until_rebuild: rebuild_freq,
            rebuild_freq,
        };
        code.rebuild();
        code
    }

    // The master rebuild function triggered periodically.
    pub fn rebuild(&mut self) {
        // 1. Dilute frequencies (adapt to recent data)
        for freq in self.freqs.iter_mut() {
            *freq = (*freq >> 1) + 1;
        }

        // 2. Build the tree to calculate optimal codeword lengths.
        self.generate_lengths();

        // 3. Convert lengths into Canonical Huffman codewords.
        self.generate_codewords();

        // 4. Repopulate the O(1) lookup table.
        self.pack_table();

        // 5. Reset the countdown
        self.syms_until_rebuild = self.rebuild_freq;
    }

    /// How many bits each symbol needs based on `self.freqs`
    fn generate_lengths(&mut self) {
        loop {
            // Collect active symbols
            let mut leaves = [0u16; NUM_SYMS];
            let mut num_leaves = 0;
            
            for i in 0..NUM_SYMS {
                if self.freqs[i] > 0 {
                    leaves[num_leaves] = i as u16;
                    num_leaves += 1;
                }
            }

            self.lens.fill(0);

            if num_leaves == 0 {
                return;
            } else if num_leaves == 1 {
                self.lens[leaves[0] as usize] = 1;
                return;
            }

            // Sort leaves by frequency in ascending order
            leaves[..num_leaves].sort_unstable_by_key(|&sym| self.freqs[sym as usize]);

            // The Two-Queue tree builder
            // internal_freqs acts as the queue for newly merged nodes.
            let mut internal_freqs = [0u32; NUM_SYMS];
            // parents array to track the tree structure for depth calculation later.
            // Indices 0..NUM_SYMS are leaves, NUM_SYMS..2*NUM_SYMS are internal nodes.
            // 2048 as a safe upper bound since max NUM_SYMS is 799
            let mut parents = [0u16; 2048];

            let mut leaf_idx = 0;
            let mut internal_head = 0;
            let mut internal_tail = 0;

            // We need to perform num_leaves - 1 merges to build the tree
            for i in 0..(num_leaves - 1) {
                // Helper to pop the minimum weight node from either queue
                let mut pop_min = || -> (u16, u32) {
                    let leaf_freq = if leaf_idx < num_leaves {
                        self.freqs[leaves[leaf_idx] as usize]
                    } else {
                        u32::MAX
                    };
                    
                    let internal_freq = if internal_head < internal_tail {
                        internal_freqs[internal_head]
                    } else {
                        u32::MAX
                    };

                    // The <= tie-breaker strictly favors leaves, ensuring a shallower tree.
                    if leaf_freq <= internal_freq {
                        let node = leaves[leaf_idx];
                        leaf_idx += 1;
                        (node, leaf_freq)
                    } else {
                        let node = (NUM_SYMS + internal_head) as u16;
                        internal_head += 1;
                        (node, internal_freq)
                    }
                };

                let (left_node, left_freq) = pop_min();
                let (right_node, right_freq) = pop_min();

                let parent_node = (NUM_SYMS + i) as u16;
                internal_freqs[i] = left_freq + right_freq;
                
                parents[left_node as usize] = parent_node;
                parents[right_node as usize] = parent_node;
                internal_tail += 1;
            }

            // Calculate depths bottom-up
            let root = (NUM_SYMS + num_leaves - 2) as u16;
            let mut max_depth = 0;

            for i in 0..num_leaves {
                let sym = leaves[i] as usize;
                let mut depth = 0;
                let mut curr = sym as u16;
                
                // Traverse up parent pointers until we hit the root
                while curr != root {
                    curr = parents[curr as usize];
                    depth += 1;
                }
                
                self.lens[sym] = depth;
                if depth > max_depth {
                    max_depth = depth;
                }
            }

            // Validate against LZMS length limits
            if max_depth <= 15 {
                break; // Tree is valid, exit the loop
            }

            // Depth exceeded 15. Dilute frequencies and try again.
            for i in 0..NUM_SYMS {
                if self.freqs[i] > 0 {
                    // Divide by 2, but ensure it never hits 0 to keep the symbol alive
                    self.freqs[i] = (self.freqs[i] >> 1).max(1);
                }
            }
        }
    }

    // Assign the actual binary codewords based on `self.lens`.
    fn generate_codewords(&mut self) {
        let mut length_counts = [0u32; 16];
        for &len in &self.lens {
            if len > 0 {
                length_counts[len as usize] += 1;
            }
        }

        let mut next_code = [0u32; 16]; // Max codeword length is 15 in LZMS
        let mut code = 0;
        for len in 1..=15 {
            code = (code + length_counts[len - 1]) << 1;
            next_code[len] = code;
        }

        for sym in 0..NUM_SYMS {
            let len = self.lens[sym] as usize;
            if len > 0 {
                self.codewords[sym] = next_code[len];
                next_code[len] += 1;
            }
        }
    }

    // Pack `codewords` and `lens` into `decode_table` for O(1) lookups.
    fn pack_table(&mut self) {
        // Clear the table
        self.decode_table.fill(0);

        // The overflow binary tree nodes start immediately after the fast-path table.
        let mut next_free_node = 1 << TABLE_BITS;

        for sym in 0..NUM_SYMS {
            let len = self.lens[sym] as usize;
            if len == 0 { continue; }
            
            let code = self.codewords[sym];
            
            if len <= TABLE_BITS {
                // Fast Path: Fill all permutations of the unused bits
                let diff = TABLE_BITS - len;
                let start = code << diff;
                let end = start + (1 << diff);
                
                let entry = ((sym as u16) << 4) | (len as u16);
                for i in start..end {
                    self.decode_table[i as usize] = entry;
                }
            } else {
                // Overflow Path: Build a binary tree for the remaining bits
                // Get the top TABLE_BITS to find the anchor point in the main table
                let prefix = code >> (len - TABLE_BITS);
                let mut curr_node = prefix as usize;
                
                // Walk down the tree for the remaining bits
                for bit_idx in (0..(len - TABLE_BITS)).rev() {
                    let bit = (code >> bit_idx) & 1;
                    
                    // If this node doesn't have children yet, allocate two slots
                    if self.decode_table[curr_node] == 0 {
                        // Shift by 4 leaves 'len' = 0, marking this as an internal node.
                        // The upper bits store the index of the left child.
                        self.decode_table[curr_node] = (next_free_node as u16) << 4;
                        next_free_node += 2; 
                    }
                    
                    // Jump to the left (0) or right (1) child
                    curr_node = ((self.decode_table[curr_node] >> 4) as usize) + (bit as usize);
                }
                
                // At the leaf, write the actual symbol and length
                self.decode_table[curr_node] = ((sym as u16) << 4) | (len as u16);
            }
        }
    }

    // Hot-path decode function
    #[inline(always)]
    pub fn decode_symbol(&mut self, bitstream: &mut BackwardBitstream) -> usize {
        // Peek the absolute maximum LZMS length (15 bits)
        let peek_bits = bitstream.peek(15);
        
        // Extract the fast-path index (top TABLE_BITS)
        let mut curr_node = (peek_bits >> (15 - TABLE_BITS)) as usize;
        let mut entry = self.decode_table[curr_node];
        let mut len = (entry & 0xF) as u32; 
        
        // If len == 0, we hit an internal node and must walk the overflow tree.
        // This loop runs max 5 times (15 - 10), making it incredibly fast.
        if len == 0 {
            let mut bit_idx = 15 - TABLE_BITS;
            loop {
                bit_idx -= 1;
                let bit = (peek_bits >> bit_idx) & 1;
                
                curr_node = ((entry >> 4) as usize) + (bit as usize);
                entry = self.decode_table[curr_node];
                len = (entry & 0xF) as u32;
                
                // If len > 0, we found the actual symbol leaf
                if len != 0 { break; }
            }
        }
        
        bitstream.consume(len);
        
        let sym = (entry >> 4) as usize;
        
        self.freqs[sym] += 1;
        self.syms_until_rebuild -= 1;
        if self.syms_until_rebuild == 0 {
            self.rebuild();
        }
        
        sym
    }
}


// -- Jump Filter

// Reverts (or apply) the x86 jump filter. For decompression, `undo` must be `true`.
pub fn lzms_x86_filter(data: &mut [u8], last_target_usages: &mut [i32; 65536], undo: bool) {
    if data.len() <= 17 {
        return;
    }

    let mut last_x86_pos = -LZMS_X86_MAX_TRANSLATION_OFFSET - 1;

    // Reset the usage tracking array
    for i in 0..65536 {
        last_target_usages[i] = -LZMS_X86_ID_WINDOW_SIZE - 1;
    }

    let mut p = 1_usize;
    let tail_ptr = data.len() - 16;

    // Optimization: we write a temporary sentinel byte near the end of the buffer 
    // to guarantee the `find_next_opcode` loop safely breaks without bounds checking.
    let saved_byte = data[tail_ptr + 8];
    data[tail_ptr + 8] = 0xE8;

    while p < tail_ptr {
        // Fast-forward to the next potential x86 opcode
        while !matches!(data[p], 0x48 | 0x4C | 0xE8 | 0xE9 | 0xF0 | 0xFF) {
            p += 1;
        }

        if p >= tail_ptr {
            break;
        }

        let mut max_trans_offset = LZMS_X86_MAX_TRANSLATION_OFFSET;
        let opcode_nbytes: usize;

        let b0 = data[p];
        let b1 = data[p + 1];
        let b2 = data[p + 2];

        // x86 Instruction decode heuristics
        if b0 >= 0xF0 {
            if (b0 & 0x0F) != 0 {
                if b1 == 0x15 { opcode_nbytes = 2; } else { p += 1; continue; }
            } else {
                if b1 == 0x83 && b2 == 0x05 { opcode_nbytes = 3; } else { p += 1; continue; }
            }
        } else if b0 <= 0x4C {
            if (b2 & 0x07) == 0x05 {
                if b1 == 0x8D || (b1 == 0x8B && (b0 & 0x04) == 0 && (b2 & 0xF0) == 0) {
                    opcode_nbytes = 3;
                } else { p += 1; continue; }
            } else { p += 1; continue; }
        } else {
            if (b0 & 0x01) != 0 {
                p += 4;
                continue;
            } else {
                opcode_nbytes = 1;
                max_trans_offset >>= 1;
            }
        }

        let i = p as i32;
        p += opcode_nbytes;

        // Read the 32-bit relative/absolute displacement
        let val = u32::from_le_bytes([data[p], data[p+1], data[p+2], data[p+3]]);
        let val_16 = u16::from_le_bytes([data[p], data[p+1]]);
        
        let target16 = (i as u32).wrapping_add(val_16 as u32) as u16 as usize;

        // Apply the translation
        if undo {
            if i - last_x86_pos <= max_trans_offset {
                let n = val.wrapping_sub(i as u32);
                data[p..p+4].copy_from_slice(&n.to_le_bytes());
            }
        } else {
            if i - last_x86_pos <= max_trans_offset {
                let n = val.wrapping_add(i as u32);
                data[p..p+4].copy_from_slice(&n.to_le_bytes());
            }
        }

        let i_updated = i + (opcode_nbytes as i32) + 3;

        if i_updated - last_target_usages[target16] <= LZMS_X86_ID_WINDOW_SIZE {
            last_x86_pos = i_updated;
        }

        last_target_usages[target16] = i_updated;

        p += 4;
    }

    // Restore the sentinel byte
    data[tail_ptr + 8] = saved_byte;
}


