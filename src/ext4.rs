use std::io::{self, Seek, SeekFrom, Write};
use rand::Rng;



const SUPERBLOCK_MAGIC: u16 = 0xEF53;
const EXTENT_HEADER_MAGIC: u16 = 0xF30A;
const FIRST_INODE: u32 = 11;
const INODE_SIZE: u32 = 256;
const EXTRA_ISIZE: u16 = 32;
const INODE_BLOCK_SIZE: usize = 60;


// ── Some helpers
fn align_up(n: usize, align: usize) -> usize { (n + align - 1) & !(align - 1) }

fn timestamp_now() -> (u32, u32) {
    let dur = std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs();
    (secs as u32, (((secs >> 32) & 0x3) as u32) | (dur.subsec_nanos() << 2))
}


// ── Ext4 Structures 

#[derive(Clone, Default)]
struct SuperBlock {
    inodes_count: u32, blocks_count_lo: u32, free_blocks_count_lo: u32, free_inodes_count: u32,
    first_data_block: u32, log_block_size: u32, log_cluster_size: u32, blocks_per_group: u32,
    clusters_per_group: u32, inodes_per_group: u32, magic: u16, state: u16, errors: u16,
    creator_os: u32, rev_level: u32,
    first_ino: u32, inode_size: u16, feature_compat: u32, feature_incompat: u32, feature_ro_compat: u32,
    uuid: [u8; 16], volume_name: [u8; 16], desc_size: u16, min_extra_isize: u16, want_extra_isize: u16,
}

impl SuperBlock {
    fn write_to(&self, buf: &mut [u8]) {
        buf.fill(0);
        buf[0..4].copy_from_slice(&self.inodes_count.to_le_bytes());
        buf[4..8].copy_from_slice(&self.blocks_count_lo.to_le_bytes());
        buf[12..16].copy_from_slice(&self.free_blocks_count_lo.to_le_bytes());
        buf[16..20].copy_from_slice(&self.free_inodes_count.to_le_bytes());
        buf[20..24].copy_from_slice(&self.first_data_block.to_le_bytes());
        buf[24..28].copy_from_slice(&self.log_block_size.to_le_bytes());
        buf[28..32].copy_from_slice(&self.log_cluster_size.to_le_bytes());
        buf[32..36].copy_from_slice(&self.blocks_per_group.to_le_bytes());
        buf[36..40].copy_from_slice(&self.clusters_per_group.to_le_bytes());
        buf[40..44].copy_from_slice(&self.inodes_per_group.to_le_bytes());
        buf[56..58].copy_from_slice(&self.magic.to_le_bytes());
        buf[58..60].copy_from_slice(&self.state.to_le_bytes());
        buf[60..62].copy_from_slice(&self.errors.to_le_bytes());
        buf[72..76].copy_from_slice(&self.creator_os.to_le_bytes());
        buf[76..80].copy_from_slice(&self.rev_level.to_le_bytes());
        buf[84..88].copy_from_slice(&self.first_ino.to_le_bytes());
        buf[88..90].copy_from_slice(&self.inode_size.to_le_bytes());
        buf[92..96].copy_from_slice(&self.feature_compat.to_le_bytes());
        buf[96..100].copy_from_slice(&self.feature_incompat.to_le_bytes());
        buf[100..104].copy_from_slice(&self.feature_ro_compat.to_le_bytes());
        buf[104..120].copy_from_slice(&self.uuid);
        buf[120..136].copy_from_slice(&self.volume_name);
        buf[254..256].copy_from_slice(&self.desc_size.to_le_bytes());
        buf[348..350].copy_from_slice(&self.min_extra_isize.to_le_bytes());
        buf[350..352].copy_from_slice(&self.want_extra_isize.to_le_bytes());
    }
}

#[derive(Clone, Default)]
struct GroupDescriptor {
    block_bitmap_lo: u32, inode_bitmap_lo: u32, inode_table_lo: u32,
    free_blocks_count_lo: u16, free_inodes_count_lo: u16, used_dirs_count_lo: u16, flags: u16,
}

impl GroupDescriptor {
    fn write_to(&self, buf: &mut [u8]) {
        buf[0..32].fill(0);
        buf[0..4].copy_from_slice(&self.block_bitmap_lo.to_le_bytes());
        buf[4..8].copy_from_slice(&self.inode_bitmap_lo.to_le_bytes());
        buf[8..12].copy_from_slice(&self.inode_table_lo.to_le_bytes());
        buf[12..14].copy_from_slice(&self.free_blocks_count_lo.to_le_bytes());
        buf[14..16].copy_from_slice(&self.free_inodes_count_lo.to_le_bytes());
        buf[16..18].copy_from_slice(&self.used_dirs_count_lo.to_le_bytes());
        buf[18..20].copy_from_slice(&self.flags.to_le_bytes());
    }
}

#[derive(Clone)]
struct Inode {
    mode: u16, uid: u16, size_lo: u32, atime: u32, ctime: u32, mtime: u32, dtime: u32, gid: u16, links_count: u16,
    blocks_lo: u32, flags: u32, block: [u8; INODE_BLOCK_SIZE],
    size_hi: u32,
    uid_hi: u16, gid_hi: u16, extra_isize: u16, ctime_extra: u32, mtime_extra: u32, atime_extra: u32, crtime: u32, crtime_extra: u32,
}

impl Default for Inode {
    fn default() -> Self {
        Self {
            mode: 0, uid: 0, size_lo: 0, atime: 0, ctime: 0, mtime: 0, dtime: 0, gid: 0, links_count: 0,
            blocks_lo: 0, flags: 0, block: [0u8; INODE_BLOCK_SIZE],
            size_hi: 0,
            uid_hi: 0, gid_hi: 0, extra_isize: 0, ctime_extra: 0, mtime_extra: 0, atime_extra: 0, crtime: 0, crtime_extra: 0,
        }
    }
}

impl Inode {
    fn root_inode() -> Self {
        let (time_lo, time_extra) = timestamp_now();
        Self {
            mode: 0x4000 | 0o755, links_count: 2, flags: 0x40000, extra_isize: EXTRA_ISIZE,
            atime: time_lo, ctime: time_lo, mtime: time_lo, crtime: time_lo,
            atime_extra: time_extra, ctime_extra: time_extra, mtime_extra: time_extra, crtime_extra: time_extra,
            ..Self::default()
        }
    }

    fn set_file_size(&mut self, size: u64) {
        self.size_lo = size as u32;
        self.size_hi = (size >> 32) as u32;
    }

    fn write_to(&self, buf: &mut [u8]) {
        buf.fill(0);
        buf[0..2].copy_from_slice(&self.mode.to_le_bytes());
        buf[2..4].copy_from_slice(&self.uid.to_le_bytes());
        buf[4..8].copy_from_slice(&self.size_lo.to_le_bytes());
        buf[8..12].copy_from_slice(&self.atime.to_le_bytes());
        buf[12..16].copy_from_slice(&self.ctime.to_le_bytes());
        buf[16..20].copy_from_slice(&self.mtime.to_le_bytes());
        buf[20..24].copy_from_slice(&self.dtime.to_le_bytes());
        buf[24..26].copy_from_slice(&self.gid.to_le_bytes());
        buf[26..28].copy_from_slice(&self.links_count.to_le_bytes());
        buf[28..32].copy_from_slice(&self.blocks_lo.to_le_bytes());
        buf[32..36].copy_from_slice(&self.flags.to_le_bytes());
        buf[40..100].copy_from_slice(&self.block);
        buf[108..112].copy_from_slice(&self.size_hi.to_le_bytes());
        buf[120..122].copy_from_slice(&self.uid_hi.to_le_bytes());
        buf[122..124].copy_from_slice(&self.gid_hi.to_le_bytes());
        buf[128..130].copy_from_slice(&self.extra_isize.to_le_bytes());
        buf[132..136].copy_from_slice(&self.ctime_extra.to_le_bytes());
        buf[136..140].copy_from_slice(&self.mtime_extra.to_le_bytes());
        buf[140..144].copy_from_slice(&self.atime_extra.to_le_bytes());
        buf[144..148].copy_from_slice(&self.crtime.to_le_bytes());
        buf[148..152].copy_from_slice(&self.crtime_extra.to_le_bytes());
    }
}


// ── Extent tree 

fn write_inline_extents(inode: &mut Inode, start: u32, data_blocks: u32) {
    let mut buf = [0u8; INODE_BLOCK_SIZE];
    buf[0..2].copy_from_slice(&EXTENT_HEADER_MAGIC.to_le_bytes());
    buf[2..4].copy_from_slice(&1u16.to_le_bytes()); // 1 entry
    buf[4..6].copy_from_slice(&4u16.to_le_bytes()); // max 4
    buf[6..8].copy_from_slice(&0u16.to_le_bytes()); // depth 0

    let off = 12;
    buf[off..off+4].copy_from_slice(&0u32.to_le_bytes()); // logical block
    buf[off+4..off+6].copy_from_slice(&(data_blocks as u16).to_le_bytes()); // len
    buf[off+6..off+8].copy_from_slice(&0u16.to_le_bytes()); // start_hi
    buf[off+8..off+12].copy_from_slice(&start.to_le_bytes()); // start_lo

    inode.block = buf;
    inode.blocks_lo = data_blocks; // Because HUGE_FILE is set
    inode.flags |= 0x80000; // EXTENTS
}

// ── Directory entries

fn write_dir_entry<W: Write>(writer: &mut W, name: &str, inode: u32, file_type: u8, left: &mut i32) -> io::Result<()> {
    let name_bytes = name.as_bytes();
    let entry_size = align_up(8 + name_bytes.len(), 4);
    
    if (*left as usize) < entry_size + 12 {
        let remaining = *left as usize;
        let mut term = [0u8; 8];
        term[4..6].copy_from_slice(&(remaining as u16).to_le_bytes());
        writer.write_all(&term)?;
        if remaining > 8 { writer.write_all(&vec![0u8; remaining - 8])?; }
        *left = 4096;
    }

    let mut header = [0u8; 8];
    header[0..4].copy_from_slice(&inode.to_le_bytes());
    header[4..6].copy_from_slice(&(entry_size as u16).to_le_bytes());
    header[6] = name_bytes.len() as u8;
    header[7] = file_type;
    writer.write_all(&header)?;
    writer.write_all(name_bytes)?;
    
    let padding = entry_size - 8 - name_bytes.len();
    if padding > 0 { writer.write_all(&[0u8; 4][..padding])?; }
    *left -= entry_size as i32;
    Ok(())
}


// ── File tree - hardcoded minimal version
struct FileTreeNode { inode: u32, name: String, children: Vec<usize> }


// ── Formatter 

pub fn format_ext4<W: Write + Seek>(mut file: W, size: u64, label: &str) -> io::Result<()> {
    let block_size = 4096u32;
    
    // Setup UUID
    let mut uuid = [0u8; 16];
    rand::rng().fill_bytes(&mut uuid);
    uuid[6] = (uuid[6] & 0x0f) | 0x40;
    uuid[8] = (uuid[8] & 0x3f) | 0x80;

    let mut vol_name = [0u8; 16];
    let label_bytes = label.as_bytes();
    let copy_len = std::cmp::min(16, label_bytes.len());
    vol_name[..copy_len].copy_from_slice(&label_bytes[..copy_len]);

    let mut inodes = vec![Inode::default(); 16];
    inodes[1] = Inode::root_inode(); // Inode 2 (Root)
    
    // Create /lost+found (Inode 11)
    let (t_lo, t_ex) = timestamp_now();
    inodes[10] = Inode {
        mode: 0x4000 | 0o700, links_count: 2, flags: 0x40000, extra_isize: EXTRA_ISIZE,
        atime: t_lo, ctime: t_lo, mtime: t_lo, crtime: t_lo,
        atime_extra: t_ex, ctime_extra: t_ex, mtime_extra: t_ex, crtime_extra: t_ex,
        ..Inode::default()
    };

    let tree = vec![
        FileTreeNode { inode: 2, name: "/".to_string(), children: vec![1] },
        FileTreeNode { inode: 11, name: "lost+found".to_string(), children: vec![] },
    ];

    let blocks_per_group = block_size * 8;
    let max_inodes_per_group = block_size * 8;
    let block_count = ((size - 1) / block_size as u64 + 1) as u32;
    let group_count = (block_count - 1) / blocks_per_group + 1;
    let groups_per_desc_block = block_size / 32;
    let group_desc_blocks = ((group_count - 1) / groups_per_desc_block + 1) * 32;

    let mut current_block = group_desc_blocks + 1;

    // - Commit Directories
    for idx in 0..tree.len() {
        let inode_num = tree[idx].inode;
        let start_block = current_block;
        
        file.seek(SeekFrom::Start(current_block as u64 * block_size as u64))?;
        let mut left = block_size as i32;

        write_dir_entry(&mut file, ".", inode_num, 2, &mut left)?;
        write_dir_entry(&mut file, "..", 2, 2, &mut left)?; // Both point to 2 in our minimal tree

        for &child_idx in &tree[idx].children {
            write_dir_entry(&mut file, &tree[child_idx].name, tree[child_idx].inode, 2, &mut left)?;
        }

        if left > 0 {
            let mut term = [0u8; 8];
            term[4..6].copy_from_slice(&(left as u16).to_le_bytes());
            file.write_all(&term)?;
            if left > 8 { file.write_all(&vec![0u8; (left - 8) as usize])?; }
        }
        
        let end_block = current_block + 1;
        inodes[(inode_num - 1) as usize].set_file_size((end_block - start_block) as u64 * block_size as u64);
        write_inline_extents(&mut inodes[(inode_num - 1) as usize], start_block, 1);
        current_block = end_block;
    }

    // - Layout Optimization
    let inc = (block_size * 512 / INODE_SIZE) as usize;
    let mut best_groups = u32::MAX;
    let mut inodes_per_group = inc as u32;
    let mut ipg = inc;
    
    while ipg <= max_inodes_per_group as usize {
        let inode_bpg = ipg as u32 * INODE_SIZE / block_size;
        let data_bpg = blocks_per_group - inode_bpg - 2;
        let min_blocks = (tree.len() as u32).saturating_sub(1) / (ipg as u32) * data_bpg + 1;
        let eff_blocks = current_block.max(min_blocks);
        let g = (eff_blocks + data_bpg - 1) / data_bpg;
        if g < best_groups { best_groups = g; inodes_per_group = ipg as u32; }
        ipg += inc;
    }
    let block_groups = best_groups;

    // - Write Inode Table
    let inode_table_offset = current_block;
    file.seek(SeekFrom::Start(current_block as u64 * block_size as u64))?;
    
    let mut inode_buf = [0u8; INODE_SIZE as usize];
    for inode in &inodes { inode.write_to(&mut inode_buf); file.write_all(&inode_buf)?; }
    
    let table_size = INODE_SIZE as u64 * block_groups as u64 * inodes_per_group as u64;
    let written = inodes.len() as u64 * INODE_SIZE as u64;
    if table_size > written {
        file.write_all(&vec![0u8; (table_size - written) as usize])?;
    }
    current_block += (table_size / block_size as u64) as u32;

    // - Write Bitmaps and Group Descriptors
    let bitmap_offset = current_block;
    let data_size = bitmap_offset + (block_groups * 2);
    let inode_table_bpg = inodes_per_group * INODE_SIZE / block_size;

    let mut total_used_blocks = 0;
    let mut group_descriptors = Vec::new();

    for group in 0..block_groups {
        let mut bitmap = vec![0u8; block_size as usize * 2];
        let group_start = group * blocks_per_group;
        let group_end = group_start + blocks_per_group;
        let mut used_blocks = 0;

        if group_end <= data_size {
            bitmap[..block_size as usize].fill(0xFF);
            used_blocks = blocks_per_group;
        } else if group_start < data_size {
            for i in 0..(data_size - group_start) {
                bitmap[(i / 8) as usize] |= 1 << (i % 8);
                used_blocks += 1;
            }
        }

        if group == 0 {
            let used_gd_blocks = (group_count - 1) / groups_per_desc_block + 1;
            for i in 0..=used_gd_blocks { bitmap[(i / 8) as usize] |= 1 << (i % 8); }
            for i in (used_gd_blocks + 1)..=group_desc_blocks {
                let was_set = (bitmap[(i / 8) as usize] >> (i % 8)) & 1;
                bitmap[(i / 8) as usize] &= !(1 << (i % 8));
                if was_set != 0 { used_blocks -= 1; }
            }
        }

        let ib_start = block_size as usize;
        let mut used_inodes = 0;
        for i in 0..inodes_per_group {
            let ino = 1 + group * inodes_per_group + i;
            if ino > inodes.len() as u32 { continue; }
            if ino > 10 && inodes[(ino - 1) as usize].links_count == 0 { continue; }
            bitmap[ib_start + (i / 8) as usize] |= 1 << (i % 8);
            used_inodes += 1;
        }
        for i in (inodes_per_group / 8)..block_size { bitmap[ib_start + i as usize] = 0xFF; }

        file.seek(SeekFrom::Start((bitmap_offset + 2 * group) as u64 * block_size as u64))?;
        file.write_all(&bitmap)?;

        group_descriptors.push(GroupDescriptor {
            block_bitmap_lo: bitmap_offset + 2 * group,
            inode_bitmap_lo: bitmap_offset + 2 * group + 1,
            inode_table_lo: inode_table_offset + group * inode_table_bpg,
            free_blocks_count_lo: (blocks_per_group - used_blocks) as u16,
            free_inodes_count_lo: (inodes_per_group - used_inodes) as u16,
            used_dirs_count_lo: if group == 0 { 2 } else { 0 },
            flags: 0,
        });
        total_used_blocks += used_blocks;
    }

    // Write extra (empty) groups if disk is larger
    let empty_bb = {
        let mut bm = vec![0u8; blocks_per_group as usize / 8];
        for i in 0..(inode_table_bpg + 2) { bm[(i / 8) as usize] |= 1 << (i % 8); }
        bm
    };
    let empty_ib = {
        let mut bm = vec![0xFFu8; blocks_per_group as usize / 8];
        for i in 0..inodes_per_group as u16 { bm[(i / 8) as usize] &= !(1 << (i % 8)); }
        bm
    };

    for group in block_groups..group_count {
        let blocks_in_group = if group == group_count - 1 {
            let rem = ((size / block_size as u64) % blocks_per_group as u64) as u32;
            if rem == 0 { blocks_per_group } else { rem }
        } else { blocks_per_group };

        let bb_offset = group * blocks_per_group + inode_table_bpg;
        
        group_descriptors.push(GroupDescriptor {
            block_bitmap_lo: bb_offset,
            inode_bitmap_lo: bb_offset + 1,
            inode_table_lo: group * blocks_per_group,
            free_blocks_count_lo: blocks_in_group.saturating_sub(inode_table_bpg + 2) as u16,
            free_inodes_count_lo: inodes_per_group as u16,
            used_dirs_count_lo: 0,
            flags: 0,
        });
        total_used_blocks += inode_table_bpg + 2;

        file.seek(SeekFrom::Start(bb_offset as u64 * block_size as u64))?;
        if group == group_count - 1 && blocks_in_group < blocks_per_group {
            let mut p_bb = vec![0u8; blocks_per_group as usize / 8];
            for i in blocks_in_group..blocks_per_group { p_bb[(i / 8) as usize] |= 1 << (i % 8); }
            for i in 0..(inode_table_bpg + 2) { p_bb[(i / 8) as usize] |= 1 << (i % 8); }
            file.write_all(&p_bb)?;
        } else {
            file.write_all(&empty_bb)?;
        }
        file.write_all(&empty_ib)?;
    }

    // - Write Group Descriptors
    file.seek(SeekFrom::Start(1 * block_size as u64))?;
    let mut gd_buf = [0u8; 32];
    for gd in &group_descriptors { gd.write_to(&mut gd_buf); file.write_all(&gd_buf)?; }

    // - Write SuperBlock
    let computed_inodes = group_count as u64 * inodes_per_group as u64;
    let mut blocks_cnt = group_count as u64 * blocks_per_group as u64;
    if blocks_cnt < total_used_blocks as u64 { blocks_cnt = total_used_blocks as u64; }
    let free_blocks = blocks_cnt.saturating_sub(total_used_blocks as u64);

    let mut sb = SuperBlock {
        inodes_count: computed_inodes as u32,
        blocks_count_lo: blocks_cnt as u32,
        free_blocks_count_lo: free_blocks as u32,
        free_inodes_count: computed_inodes as u32 - 11, // 11 used (1..11)
        first_data_block: 0,
        log_block_size: 2, // 4096
        log_cluster_size: 2,
        blocks_per_group,
        clusters_per_group: blocks_per_group,
        inodes_per_group,
        magic: SUPERBLOCK_MAGIC,
        state: 1, errors: 1, creator_os: 3, rev_level: 1,
        first_ino: FIRST_INODE,
        inode_size: INODE_SIZE as u16,
        feature_compat: 0x208, // SPARSE_SUPER2 | EXT_ATTR
        feature_incompat: 0x42, // FILETYPE | EXTENTS
        feature_ro_compat: 0x4A, // LARGE_FILE | HUGE_FILE | EXTRA_ISIZE
        min_extra_isize: EXTRA_ISIZE,
        want_extra_isize: EXTRA_ISIZE,
        uuid,
        desc_size: 64, 
        ..SuperBlock::default()
    };
    sb.volume_name = vol_name;

    file.seek(SeekFrom::Start(0))?;
    file.write_all(&[0u8; 1024])?;
    let mut sb_buf = [0u8; 1024];
    sb.write_to(&mut sb_buf);
    file.write_all(&sb_buf)?;
    file.write_all(&[0u8; 2048])?;

    file.flush()?;
    Ok(())
}