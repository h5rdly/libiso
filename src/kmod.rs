use std::{
    io::{self, Seek, SeekFrom, Write},
    collections::BTreeMap,
};


// The holy texts of libkmod
const INDEX_MAGIC: u32 = 0xB007F457;
const INDEX_VERSION_MAJOR: u32 = 0x0002;
const INDEX_VERSION_MINOR: u32 = 0x0001;
const INDEX_VERSION: u32 = (INDEX_VERSION_MAJOR << 16) | INDEX_VERSION_MINOR;

const INDEX_NODE_FLAGS: u32 = 0xF0000000;
const INDEX_NODE_PREFIX: u32 = 0x80000000;
const INDEX_NODE_VALUES: u32 = 0x40000000;
const INDEX_NODE_CHILDS: u32 = 0x20000000;
const INDEX_NODE_MASK: u32 = 0x0FFFFFFF;


#[derive(Debug, Clone)]
pub struct IndexValue {
    pub priority: u32,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct IndexNode {
    pub prefix: String,
    pub values: Vec<IndexValue>,
    // kmod only supports 7-bit ASCII, so u8 is perfect for branch routing
    pub children: BTreeMap<u8, Box<IndexNode>>,
}

impl IndexNode {
    pub fn new(prefix: String) -> Self {
        Self {
            prefix,
            values: Vec::new(),
            children: BTreeMap::new(),
        }
    }

    pub fn add_value(&mut self, value: String, priority: u32) {
        // kmod orders values by priority ascending.
        let pos = self.values.iter().position(|v| v.priority >= priority).unwrap_or(self.values.len());
        
        // Prevent duplicate aliases/deps per kmod logic
        if !self.values.iter().any(|v| v.value == value) {
            self.values.insert(pos, IndexValue { priority, value });
        }
    }
}

pub struct KmodIndex {
    root: Box<IndexNode>,
}

impl KmodIndex {
    pub fn new() -> Self {
        Self {
            root: Box::new(IndexNode::new(String::new())),
        }
    }

    /// Inserts a key-value pair into the Patricia Trie.
    pub fn insert(&mut self, key: &str, value: &str, priority: u32) {
        let mut current = &mut self.root;
        let mut key_bytes = key.as_bytes();

        loop {
            let prefix_bytes = current.prefix.as_bytes();
            let mut common_len = 0;
            
            // 1. Find the longest common prefix
            while common_len < prefix_bytes.len() && common_len < key_bytes.len() && prefix_bytes[common_len] == key_bytes[common_len] {
                common_len += 1;
            }

            // 2. If the key diverges from the current node's prefix, we must split the node!
            if common_len < prefix_bytes.len() {
                let mut new_child = Box::new(IndexNode::new(
                    String::from_utf8(prefix_bytes[common_len + 1..].to_vec()).unwrap()
                ));
                
                // The new child inherits the current node's payload
                new_child.values = std::mem::take(&mut current.values);
                new_child.children = std::mem::take(&mut current.children);

                let split_char = prefix_bytes[common_len];
                current.prefix = String::from_utf8(prefix_bytes[..common_len].to_vec()).unwrap();
                current.children.insert(split_char, new_child);
            }

            // 3. If the key perfectly matches the (possibly newly split) prefix
            if common_len == key_bytes.len() {
                current.add_value(value.to_string(), priority);
                return;
            }

            // 4. Otherwise, the key continues past the prefix
            key_bytes = &key_bytes[common_len..];
            let next_char = key_bytes[0];
            key_bytes = &key_bytes[1..];

            // 5. If there is no child for the next character, create one!
            if !current.children.contains_key(&next_char) {
                let mut new_child = Box::new(IndexNode::new(
                    String::from_utf8(key_bytes.to_vec()).unwrap()
                ));
                new_child.add_value(value.to_string(), priority);
                current.children.insert(next_char, new_child);
                return;
            }

            // 6. Descend into the existing child and repeat
            current = current.children.get_mut(&next_char).unwrap();
        }
    }

    /// Serializes the Trie into the kmod binary format
    pub fn write<W: Write + Seek>(&self, writer: &mut W) -> io::Result<()> {
        // kmod uses Network Byte Order (Big Endian) for everything!
        writer.write_all(&INDEX_MAGIC.to_be_bytes())?;
        writer.write_all(&INDEX_VERSION.to_be_bytes())?;
        
        let initial_offset = writer.stream_position()?;
        writer.write_all(&0u32.to_be_bytes())?; // Placeholder for root offset

        // Begin the recursive post-order write
        let root_offset = self.write_node(&self.root, writer)?;

        // Go back and patch the root offset into the header
        let final_offset = writer.stream_position()?;
        writer.seek(SeekFrom::Start(initial_offset))?;
        writer.write_all(&root_offset.to_be_bytes())?;
        writer.seek(SeekFrom::Start(final_offset))?;

        Ok(())
    }

    fn write_node<W: Write + Seek>(&self, node: &IndexNode, writer: &mut W) -> io::Result<u32> {
        let mut child_offs = Vec::new();
        let mut first = 128u8;
        let mut last = 0u8;

        // kmod expects a post-order traversal (children written BEFORE their parent)
        if !node.children.is_empty() {
            first = *node.children.keys().min().unwrap();
            last = *node.children.keys().max().unwrap();

            // kmod expects continuous padding between the first and last child bounds
            for ch in first..=last {
                if let Some(child) = node.children.get(&ch) {
                    child_offs.push(self.write_node(child, writer)?);
                } else {
                    child_offs.push(0); // Null pointer padding
                }
            }
        }

        // Now write THIS node
        let mut offset = writer.stream_position()? as u32;

        if !node.prefix.is_empty() {
            writer.write_all(node.prefix.as_bytes())?;
            writer.write_all(&[0])?; // Null terminated
            offset |= INDEX_NODE_PREFIX;
        }

        if !child_offs.is_empty() {
            writer.write_all(&[first, last])?;
            for coff in child_offs {
                writer.write_all(&coff.to_be_bytes())?;
            }
            offset |= INDEX_NODE_CHILDS;
        }

        if !node.values.is_empty() {
            writer.write_all(&(node.values.len() as u32).to_be_bytes())?;
            for val in &node.values {
                writer.write_all(&val.priority.to_be_bytes())?;
                writer.write_all(val.value.as_bytes())?;
                writer.write_all(&[0])?; // Null terminated
            }
            offset |= INDEX_NODE_VALUES;
        }

        Ok(offset)
    }
}



// -- Kmod Parser

// A bare-metal, no_std, zero-allocation parser for modules.dep.bin
pub struct KmodBinaryParser<'a> {
    data: &'a [u8],
}

impl<'a> KmodBinaryParser<'a> {
    pub fn new(data: &'a [u8]) -> Result<Self, &'static str> {
        if data.len() < 12 {
            return Err("File too small");
        }
        
        // Read Magic: 0xB007F457
        let magic = u32::from_be_bytes(data[0..4].try_into().unwrap());
        if magic != 0xB007F457 {
            return Err("Invalid kmod magic");
        }

        Ok(Self { data })
    }

    /// Recursively hops through the byte slice to find a module
    pub fn search(&self, target_module: &str) -> Option<&'a str> {
        let root_offset = u32::from_be_bytes(self.data[8..12].try_into().unwrap());
        self.walk_node(root_offset & 0x0FFFFFFF, target_module.as_bytes())
    }

    fn walk_node(&self, offset: u32, mut search_key: &[u8]) -> Option<&'a str> {
        let flags = offset & 0xF0000000;
        let mut curr_idx = (offset & 0x0FFFFFFF) as usize;

        // 1. Check Prefix
        if flags & 0x80000000 != 0 {
            // Find null terminator for the prefix string
            let mut prefix_len = 0;
            while self.data[curr_idx + prefix_len] != 0 { prefix_len += 1; }
            let prefix = &self.data[curr_idx..curr_idx + prefix_len];
            curr_idx += prefix_len + 1;

            // If the search key doesn't match the prefix, wrong branch
            if !search_key.starts_with(prefix) {
                return None;
            }
            // Consume the matched prefix
            search_key = &search_key[prefix.len()..];
        }

        // 2. Exact Match Found? Check Values
        if search_key.is_empty() {
            if flags & 0x40000000 != 0 {
                // Skip the child array to get to the values
                if flags & 0x20000000 != 0 {
                    let first = self.data[curr_idx];
                    let last = self.data[curr_idx + 1];
                    let child_count = (last - first + 1) as usize;
                    curr_idx += 2 + (child_count * 4);
                }
                
                // Read value count
                let val_count = u32::from_be_bytes(self.data[curr_idx..curr_idx+4].try_into().unwrap());
                curr_idx += 4;
                
                if val_count > 0 {
                    curr_idx += 4; // Skip priority u32
                    
                    // Read string until null terminator
                    let mut val_len = 0;
                    while self.data[curr_idx + val_len] != 0 { val_len += 1; }
                    
                    // Return the zero-copy string slice!
                    return core::str::from_utf8(&self.data[curr_idx..curr_idx + val_len]).ok();
                }
            }
            return None;
        }

        // 3. Keep Descending
        if flags & 0x20000000 != 0 {
            let first = self.data[curr_idx];
            let last = self.data[curr_idx + 1];
            curr_idx += 2;

            let next_char = search_key[0];
            if next_char >= first && next_char <= last {
                let child_idx = (next_char - first) as usize;
                let child_offset_pos = curr_idx + (child_idx * 4);
                let child_offset = u32::from_be_bytes(self.data[child_offset_pos..child_offset_pos+4].try_into().unwrap());
                
                if child_offset != 0 {
                    return self.walk_node(child_offset, search_key);
                }
            }
        }

        None
    }


    // Parses the entire binary kmod file and reconstructs a mutable KmodIndex.
    // This enables a full Binary Read -> Modify -> Write pipeline!
    pub fn to_index(&self) -> Result<KmodIndex, &'static str> {
        let mut index = KmodIndex::new();
        let root_offset = u32::from_be_bytes(self.data[8..12].try_into().unwrap());
        self.extract_node_to_index(root_offset & INDEX_NODE_MASK, String::new(), &mut index)?;
        Ok(index)
    }

    
    // Recursively walks the binary trie and populates the KmodIndex
    fn extract_node_to_index(
        &self, 
        offset: u32, 
        mut current_path: String, 
        index: &mut KmodIndex
    ) -> Result<(), &'static str> {
        let flags = offset & INDEX_NODE_FLAGS;
        let mut curr_idx = (offset & INDEX_NODE_MASK) as usize;

        // 1. Check Prefix
        if flags & INDEX_NODE_PREFIX != 0 {
            let mut prefix_len = 0;
            while self.data[curr_idx + prefix_len] != 0 { prefix_len += 1; }
            let prefix = core::str::from_utf8(&self.data[curr_idx..curr_idx + prefix_len])
                .map_err(|_| "Invalid UTF-8 in prefix")?;
            
            current_path.push_str(prefix);
            curr_idx += prefix_len + 1;
        }

        // 2. Check Values (Extract them into the new index)
        if flags & INDEX_NODE_VALUES != 0 {
            let mut val_idx = curr_idx;
            
            // If there's a child array before the values, we must skip over it to read the values
            if flags & INDEX_NODE_CHILDS != 0 {
                let first = self.data[val_idx];
                let last = self.data[val_idx + 1];
                let child_count = (last - first + 1) as usize;
                val_idx += 2 + (child_count * 4); // skip bounds + 32-bit pointers
            }

            let val_count = u32::from_be_bytes(self.data[val_idx..val_idx+4].try_into().unwrap());
            val_idx += 4;

            for _ in 0..val_count {
                let priority = u32::from_be_bytes(self.data[val_idx..val_idx+4].try_into().unwrap());
                val_idx += 4;

                let mut val_len = 0;
                while self.data[val_idx + val_len] != 0 { val_len += 1; }
                let value_str = core::str::from_utf8(&self.data[val_idx..val_idx + val_len])
                    .map_err(|_| "Invalid UTF-8 in value")?;
                
                // BOOM! Insert the existing dependency into our mutable tree
                index.insert(&current_path, value_str, priority);
                
                val_idx += val_len + 1;
            }
        }

        // 3. Traverse Children
        if flags & INDEX_NODE_CHILDS != 0 {
            let first = self.data[curr_idx];
            let last = self.data[curr_idx + 1];
            curr_idx += 2;

            // Iterate through every possible branch character
            for ch in first..=last {
                let child_idx = (ch - first) as usize;
                let child_offset_pos = curr_idx + (child_idx * 4);
                let child_offset = u32::from_be_bytes(self.data[child_offset_pos..child_offset_pos+4].try_into().unwrap());

                // If it's not a null-pointer padding, recurse!
                if child_offset != 0 {
                    let mut next_path = current_path.clone();
                    next_path.push(ch as char);
                    self.extract_node_to_index(child_offset, next_path, index)?;
                }
            }
        }

        Ok(())
    }

}