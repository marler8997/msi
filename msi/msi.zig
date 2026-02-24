pub const InstallOptions = struct {
    msi_content: []const u8,
    /// Directory containing ".cab" files, typically the same directory where the ".msi" file is.
    cabs_dir: []const u8,
    install_path: []const u8,
};

pub fn install(scratch: std.mem.Allocator, opt: InstallOptions) !void {
    var cfb = try Cfb.parse(opt.msi_content);
    try cfb.loadStructures(scratch);

    var cab_stream_index: ?usize = null;
    for (cfb.dir_entries.?, 0..) |entry, i| {
        if (entry.object_type == 0) continue;

        if (entry.stream_size > 100000 and try entry.getObjectType() == .stream) {
            if (cab_stream_index == null) {
                cab_stream_index = i;
            }
        }
    }

    var msi_db = try MsiDatabase.parse(&cfb, scratch);
    defer msi_db.deinit();

    if (msi_db.media_table.count() > 0) {
        const cab_start = try std.time.Instant.now();

        var maybe_cabs_dir: ?std.fs.Dir = null;
        defer if (maybe_cabs_dir) |*d| d.close();

        var media_iter = msi_db.media_table.valueIterator();
        var found_external = false;
        while (media_iter.next()) |media_entry| {
            if (media_entry.cabinet.len > 0) {
                const cab_data = blk: {
                    if (maybe_cabs_dir == null) {
                        maybe_cabs_dir = try std.fs.cwd().openDir(opt.cabs_dir, .{});
                    }
                    const file = maybe_cabs_dir.?.openFile(media_entry.cabinet, .{}) catch |err| {
                        log.err("open cab file '{s}' failed with {}", .{ media_entry.cabinet, err });
                        return error.MissingCabFile;
                    };
                    defer file.close();
                    break :blk try file.readToEndAlloc(scratch, 1000 * 1024 * 1024); // 1000MB max
                };
                try std.fs.cwd().makePath(opt.install_path);
                try extractCab(cab_data, opt.install_path, scratch, &msi_db);
                found_external = true;
            }
        }

        if (found_external) {
            const cab_elapsed = (try std.time.Instant.now()).since(cab_start);
            log.info("CAB extraction took {d:.3} seconds", .{@as(f32, @floatFromInt(cab_elapsed)) / std.time.ns_per_s});
            return;
        }
    }

    if (cab_stream_index) |cab_idx| {
        const cab_entry = cfb.dir_entries.?[cab_idx];
        log.info("trying embedded CAB from stream index {}...", .{cab_idx});

        const cab_data = try cfb.readStream(cab_entry, scratch);
        defer scratch.free(cab_data);

        // Verify it's a CAB file (signature: 'MSCF' = 0x4D534346)
        if (cab_data.len >= 4) {
            const sig = std.mem.readInt(u32, cab_data[0..4], .little);
            if (sig == 0x4D534346) {
                log.info("confirmed CAB file signature (MSCF), size={} bytes", .{cab_data.len});
                try std.fs.cwd().makePath(opt.install_path);
                try extractCab(cab_data, opt.install_path, scratch, &msi_db);
                log.info("MSI installation complete", .{});
            } else {
                log.err("stream is not a CAB file (signature: 0x{X:0>8}, expected 0x4D534346)", .{sig});
                return error.NotACabFile;
            }
        }
    } else {
        log.err("no CAB file found (neither external nor embedded)", .{});
        return error.NoCabFound;
    }
}

const CabHeaderFlags = packed struct(u16) {
    has_prev: bool,
    has_next: bool,
    has_reserve: bool,
    _reserved: u13 = 0,
};

fn extractCab(cab_data: []const u8, target_dir: []const u8, allocator: std.mem.Allocator, msi_db: *MsiDatabase) !void {
    if (cab_data.len < 36) return error.InvalidCabFile;

    if (!std.mem.eql(u8, cab_data[0..4], "MSCF"))
        return error.InvalidCabSignature;

    const files_offset = std.mem.readInt(u32, cab_data[16..20], .little);
    const version_minor = cab_data[24];
    const version_major = cab_data[25];

    if (version_major != 1 or version_minor != 3) {
        log.err("unsupported CAB version: {}.{} (expected 1.3)", .{ version_major, version_minor });
        return error.UnsupportedCabVersion;
    }

    const num_folders = std.mem.readInt(u16, cab_data[26..28], .little);
    const num_files = std.mem.readInt(u16, cab_data[28..30], .little);
    const flags: CabHeaderFlags = @bitCast(std.mem.readInt(u16, cab_data[30..32], .little));

    if (flags._reserved != 0) {
        log.warn("CAB header has non-zero reserved bits: 0x{x:0>4}", .{flags._reserved});
    }

    if (flags.has_prev) {
        log.err("CAB file has previous cabinet - multi-cabinet archives not supported", .{});
        return error.UnsupportedMultiCabinet;
    }
    if (flags.has_next) {
        log.err("CAB file has next cabinet - multi-cabinet archives not supported", .{});
        return error.UnsupportedMultiCabinet;
    }

    var offset: usize = 36;

    if (flags.has_reserve) {
        if (offset + 4 > cab_data.len) return error.InvalidCabFile;
        const header_reserved = std.mem.readInt(u16, cab_data[offset..][0..2], .little);
        offset += 4 + header_reserved;
    }

    const folders_offset = offset;
    offset += @as(usize, num_folders) * 8;

    if (files_offset != offset) {
        offset = files_offset;
    }

    for (0..num_folders) |folder_idx| {
        const folder_offset = folders_offset + folder_idx * 8;
        if (folder_offset + 8 > cab_data.len) return error.InvalidCabFile;

        const data_offset = std.mem.readInt(u32, cab_data[folder_offset..][0..4], .little);
        const num_data_blocks = std.mem.readInt(u16, cab_data[folder_offset + 4 ..][0..2], .little);
        const compression_type_raw = std.mem.readInt(u16, cab_data[folder_offset + 6 ..][0..2], .little);
        const compression_type: CabCompressionType = @bitCast(compression_type_raw);

        _ = num_data_blocks;
        _ = data_offset;
        if (compression_type.method != .none and compression_type.method != .mszip) {
            log.err("unsupported CAB compression method in folder {}: {s}", .{ folder_idx, @tagName(compression_type.method) });
            return error.UnsupportedCompression;
        }
    }

    var file_iter = CabFileIterator{
        .cab_data = cab_data,
        .offset = offset,
        .remaining = num_files,
    };

    var current_folder: ?u16 = null;
    var uncompressed_data: ?[]u8 = null;
    defer if (uncompressed_data) |data| allocator.free(data);

    var extracted_count: usize = 0;
    var skipped_count: usize = 0;

    while (try file_iter.next()) |file_entry| {
        if (current_folder == null or current_folder.? != file_entry.folder_index) {
            if (uncompressed_data) |data| {
                allocator.free(data);
                uncompressed_data = null;
            }
            uncompressed_data = try decompressFolder(cab_data, file_entry.folder_index, folders_offset, allocator);
            current_folder = file_entry.folder_index;
        }

        const data = uncompressed_data.?;
        if (file_entry.folder_offset + file_entry.uncompressed_size > data.len) {
            log.err("file {s} exceeds folder data: offset={}, size={}, folder_data_len={}", .{ file_entry.filename, file_entry.folder_offset, file_entry.uncompressed_size, data.len });
            return error.CabFileDataOutOfBounds;
        }

        const file_data = data[file_entry.folder_offset..][0..file_entry.uncompressed_size];

        const file_info = msi_db.file_table.get(file_entry.filename);

        if (file_info) |fe| {
            const actual_name = blk: {
                if (std.mem.indexOf(u8, fe.file_name, "|")) |pipe_pos| {
                    break :blk fe.file_name[pipe_pos + 1 ..];
                } else {
                    break :blk fe.file_name;
                }
            };

            const comp_entry = msi_db.component_table.get(fe.component);

            if (comp_entry) |comp| {
                const resolved_dir_path = try msi_db.resolveDirectoryPath(comp.directory, allocator);

                var path_buf: [std.fs.max_path_bytes]u8 = undefined;
                const full_target_dir = try std.fmt.bufPrint(&path_buf, "{s}{s}{s}", .{ target_dir, std.fs.path.sep_str, resolved_dir_path });

                try std.fs.cwd().makePath(full_target_dir);

                var full_path_buf: [std.fs.max_path_bytes]u8 = undefined;
                const full_path = try std.fmt.bufPrint(&full_path_buf, "{s}{s}{s}", .{ full_target_dir, std.fs.path.sep_str, actual_name });

                const out_file = std.fs.cwd().createFile(full_path, .{}) catch |err| switch (err) {
                    error.PathAlreadyExists => {
                        log.warn("skipping existing file: {s}", .{full_path});
                        skipped_count += 1;
                        continue;
                    },
                    else => return err,
                };
                defer out_file.close();
                try out_file.writeAll(file_data);
                extracted_count += 1;
            } else {
                var path_buf: [std.fs.max_path_bytes]u8 = undefined;
                const full_path = try std.fmt.bufPrint(&path_buf, "{s}{s}{s}", .{ target_dir, std.fs.path.sep_str, actual_name });

                const out_file = std.fs.cwd().createFile(full_path, .{}) catch |err| switch (err) {
                    error.PathAlreadyExists => {
                        log.warn("skipping existing file: {s}", .{full_path});
                        skipped_count += 1;
                        continue;
                    },
                    else => return err,
                };
                defer out_file.close();
                try out_file.writeAll(file_data);
                extracted_count += 1;
            }
        } else {
            var path_buf: [std.fs.max_path_bytes]u8 = undefined;
            const full_path = try std.fmt.bufPrint(&path_buf, "{s}{s}{s}", .{ target_dir, std.fs.path.sep_str, file_entry.filename });

            if (std.fs.path.dirname(full_path)) |dir| {
                try std.fs.cwd().makePath(dir);
            }

            const out_file = std.fs.cwd().createFile(full_path, .{}) catch |err| switch (err) {
                error.PathAlreadyExists => {
                    skipped_count += 1;
                    continue;
                },
                else => return err,
            };
            defer out_file.close();
            try out_file.writeAll(file_data);
            extracted_count += 1;
        }
    }

    log.info("extracted {} files from cab, skipped {} duplicates", .{ extracted_count, skipped_count });
}

const CabFileIterator = struct {
    cab_data: []const u8,
    offset: usize,
    remaining: usize,

    const Entry = struct {
        filename: []const u8,
        uncompressed_size: u32,
        folder_index: u16,
        folder_offset: u32,
    };

    fn next(self: *CabFileIterator) !?Entry {
        if (self.remaining == 0) return null;

        if (self.offset + 16 > self.cab_data.len) return error.InvalidCabFile;

        const uncompressed_size = std.mem.readInt(u32, self.cab_data[self.offset..][0..4], .little);
        const folder_offset_in_cab = std.mem.readInt(u32, self.cab_data[self.offset + 4 ..][0..4], .little);
        const folder_index = std.mem.readInt(u16, self.cab_data[self.offset + 8 ..][0..2], .little);
        const date = std.mem.readInt(u16, self.cab_data[self.offset + 10 ..][0..2], .little);
        const time = std.mem.readInt(u16, self.cab_data[self.offset + 12 ..][0..2], .little);
        const attribs = std.mem.readInt(u16, self.cab_data[self.offset + 14 ..][0..2], .little);

        if (date == 0 or time == 0) {
            // log.warn("CAB file entry has zero timestamp: date=0x{x:0>4}, time=0x{x:0>4}", .{ date, time });
        }
        const unknown_bits = attribs & 0xFFC0;
        if (unknown_bits != 0) {
            // log.warn("CAB file entry has unknown attribute bits: 0x{x:0>4}", .{unknown_bits});
        }

        self.offset += 16;

        const filename_start = self.offset;
        while (self.offset < self.cab_data.len and self.cab_data[self.offset] != 0) : (self.offset += 1) {}
        if (self.offset >= self.cab_data.len) return error.InvalidCabFile;

        const filename = self.cab_data[filename_start..self.offset];
        self.offset += 1;

        self.remaining -= 1;

        return Entry{
            .filename = filename,
            .uncompressed_size = uncompressed_size,
            .folder_index = folder_index,
            .folder_offset = folder_offset_in_cab,
        };
    }
};

const CabCompressionType = packed struct(u16) {
    method: enum(u4) {
        none = 0,
        mszip = 1,
        quantum = 2,
        lzx = 3,
    },
    level: u12,
};

fn decompressFolder(cab_data: []const u8, folder_index: u16, folders_offset: usize, allocator: std.mem.Allocator) ![]u8 {
    const folder_offset = folders_offset + @as(usize, folder_index) * 8;
    if (folder_offset + 8 > cab_data.len) return error.InvalidCabFile;

    const data_offset = std.mem.readInt(u32, cab_data[folder_offset..][0..4], .little);
    const num_data_blocks = std.mem.readInt(u16, cab_data[folder_offset + 4 ..][0..2], .little);
    const compression_type: CabCompressionType = @bitCast(std.mem.readInt(u16, cab_data[folder_offset + 6 ..][0..2], .little));

    var total_uncompressed: usize = 0;
    var scan_offset = data_offset;
    for (0..num_data_blocks) |_| {
        if (scan_offset + 8 > cab_data.len) return error.InvalidCabFile;
        const compressed_size = std.mem.readInt(u16, cab_data[scan_offset + 4 ..][0..2], .little);
        const uncompressed_size = std.mem.readInt(u16, cab_data[scan_offset + 6 ..][0..2], .little);
        total_uncompressed += uncompressed_size;
        scan_offset += 8 + compressed_size;
    }

    const result_buf = try allocator.alloc(u8, total_uncompressed);
    errdefer allocator.free(result_buf);
    var result_pos: usize = 0;

    var decompress_window: []u8 = &.{};
    var sliding_window: []u8 = &.{};
    if (compression_type.method == .mszip) {
        decompress_window = try allocator.alloc(u8, stdfork.compress.flate.max_window_len);
        sliding_window = try allocator.alloc(u8, stdfork.compress.flate.history_len);
    }
    defer if (decompress_window.len > 0) allocator.free(decompress_window);
    defer if (sliding_window.len > 0) allocator.free(sliding_window);

    var block_offset = data_offset;
    var sliding_window_size: usize = 0;

    for (0..num_data_blocks) |block_num| {
        if (block_offset + 8 > cab_data.len) return error.InvalidCabFile;

        const checksum = std.mem.readInt(u32, cab_data[block_offset..][0..4], .little);
        const compressed_size = std.mem.readInt(u16, cab_data[block_offset + 4 ..][0..2], .little);
        const uncompressed_size = std.mem.readInt(u16, cab_data[block_offset + 6 ..][0..2], .little);

        _ = checksum;

        block_offset += 8;

        if (block_offset + compressed_size > cab_data.len) return error.InvalidCabFile;
        const compressed_data = cab_data[block_offset..][0..compressed_size];
        block_offset += compressed_size;

        switch (compression_type.method) {
            .none => {
                @memcpy(result_buf[result_pos..][0..compressed_data.len], compressed_data);
                result_pos += compressed_data.len;
            },
            .mszip => {
                // MSZIP format: 2-byte signature 'CK' (0x4B43) followed by deflate data
                if (compressed_data.len < 2) return error.InvalidMszipData;
                if (compressed_data[0] != 'C' or compressed_data[1] != 'K') return error.InvalidMszipSignature;

                // Copy sliding window into decompressor buffer for backreferences
                if (block_num > 0) {
                    @memcpy(decompress_window[0..sliding_window_size], sliding_window[0..sliding_window_size]);
                }

                var input_reader = std15.Io.Reader.fixed(compressed_data[2..]);
                var decompress = if (block_num == 0)
                    stdfork.compress.flate.Decompress.init(&input_reader, .raw, decompress_window)
                else
                    stdfork.compress.flate.Decompress.initPreservingWindow(&input_reader, .raw, decompress_window, sliding_window_size);

                const decompressed_block = result_buf[result_pos..][0..uncompressed_size];
                try decompress.reader.readSliceAll(decompressed_block);
                result_pos += uncompressed_size;

                if (uncompressed_size >= stdfork.compress.flate.history_len) {
                    @memcpy(sliding_window, decompressed_block[uncompressed_size - stdfork.compress.flate.history_len ..]);
                    sliding_window_size = stdfork.compress.flate.history_len;
                } else if (sliding_window_size + uncompressed_size <= stdfork.compress.flate.history_len) {
                    @memcpy(sliding_window[sliding_window_size..][0..uncompressed_size], decompressed_block);
                    sliding_window_size += uncompressed_size;
                } else {
                    const keep_from_old = stdfork.compress.flate.history_len - uncompressed_size;
                    vers.memmove(sliding_window[0..keep_from_old], sliding_window[sliding_window_size - keep_from_old .. sliding_window_size]);
                    @memcpy(sliding_window[keep_from_old..stdfork.compress.flate.history_len], decompressed_block);
                    sliding_window_size = stdfork.compress.flate.history_len;
                }
            },
            .quantum, .lzx => {
                log.err("unsupported CAB compression method: {s}", .{@tagName(compression_type.method)});
                return error.UnsupportedCompression;
            },
        }
    }

    return result_buf;
}

// Compound File Binary (CFB) Format Parser
const Cfb = struct {
    content: []const u8,
    header: Header,
    sector_size: u32,
    mini_sector_size: u32,
    fat: ?[]u32 = null,
    mini_fat: ?[]u32 = null,
    dir_entries: ?[]DirEntry = null,
    mini_stream_data: ?[]u8 = null,

    const Header = struct {
        major_version: u16,
        sector_shift: u16,
        mini_sector_shift: u16,
        num_fat_sectors: u32,
        first_dir_sector: u32,
        mini_stream_cutoff: u32,
        first_mini_fat_sector: u32,
        num_mini_fat_sectors: u32,
        first_difat_sector: u32,
        num_difat_sectors: u32,
        difat: [109]u32,

        fn parse(data: []const u8) !Header {
            if (data.len < 512) return error.InvalidHeader;

            const expected_sig = [_]u8{ 0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1 };
            if (!std.mem.eql(u8, data[0..8], &expected_sig)) {
                log.err("invalid CFB signature: expected D0CF11E0A1B11AE1, got {X:0>2}{X:0>2}{X:0>2}{X:0>2}{X:0>2}{X:0>2}{X:0>2}{X:0>2}", .{
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                });
                return error.InvalidSignature;
            }

            const major_version = std.mem.readInt(u16, data[26..][0..2], .little);
            if (major_version != 3 and major_version != 4) {
                log.err("unsupported CFB version: {}", .{major_version});
                return error.UnsupportedVersion;
            }

            const byte_order = std.mem.readInt(u16, data[28..][0..2], .little);
            if (byte_order != 0xFFFE) {
                log.err("invalid byte order: expected 0xFFFE, got 0x{X:0>4}", .{byte_order});
                return error.InvalidByteOrder;
            }

            var header: Header = undefined;
            header.major_version = major_version;
            header.sector_shift = std.mem.readInt(u16, data[30..][0..2], .little);
            header.mini_sector_shift = std.mem.readInt(u16, data[32..][0..2], .little);

            // Validate sector shifts
            if (header.sector_shift != 9 and header.sector_shift != 12) {
                log.err("invalid CFB sector shift: {} (expected 9 for 512-byte sectors or 12 for 4096-byte sectors)", .{header.sector_shift});
                return error.InvalidSectorShift;
            }
            if (header.mini_sector_shift != 6) {
                log.err("invalid CFB mini sector shift: {} (expected 6 for 64-byte sectors)", .{header.mini_sector_shift});
                return error.InvalidMiniSectorShift;
            }

            // Skip: reserved (34-39), num_dir_sectors (40-43)
            header.num_fat_sectors = std.mem.readInt(u32, data[44..][0..4], .little);
            header.first_dir_sector = std.mem.readInt(u32, data[48..][0..4], .little);
            // Skip: transaction_sig (52-55)
            header.mini_stream_cutoff = std.mem.readInt(u32, data[56..][0..4], .little);
            header.first_mini_fat_sector = std.mem.readInt(u32, data[60..][0..4], .little);
            header.num_mini_fat_sectors = std.mem.readInt(u32, data[64..][0..4], .little);
            header.first_difat_sector = std.mem.readInt(u32, data[68..][0..4], .little);
            header.num_difat_sectors = std.mem.readInt(u32, data[72..][0..4], .little);

            // Read DIFAT array (109 entries)
            for (0..109) |i| {
                const offset = 76 + i * 4;
                header.difat[i] = std.mem.readInt(u32, data[offset..][0..4], .little);
            }

            return header;
        }
    };

    const DirEntry = struct {
        name: [32]u16,
        name_len: u16,
        object_type: u8,
        start_sector: u32,
        stream_size: u64,

        const ENTRY_SIZE = 128;
        const UNUSED: u32 = 0xFFFFFFFF;
        const END_OF_CHAIN: u32 = 0xFFFFFFFE;
        const FAT_SECTOR: u32 = 0xFFFFFFFD;
        const DIFAT_SECTOR: u32 = 0xFFFFFFFC;

        const ObjectType = enum(u8) {
            unknown = 0x00,
            storage = 0x01,
            stream = 0x02,
            root = 0x05,
        };

        fn parse(data: []const u8) !DirEntry {
            if (data.len < ENTRY_SIZE) return error.InvalidDirEntry;

            var entry: DirEntry = undefined;

            // Read name (64 bytes, 32 UTF-16 characters)
            for (0..32) |i| {
                entry.name[i] = std.mem.readInt(u16, data[i * 2 ..][0..2], .little);
            }

            entry.name_len = std.mem.readInt(u16, data[64..][0..2], .little);
            entry.object_type = data[66];
            // Skip: color (67), left_sibling (68-71), right_sibling (72-75), child (76-79)
            // Skip: CLSID (80-95), state_bits (96-99), creation_time (100-107), modified_time (108-115)
            entry.start_sector = std.mem.readInt(u32, data[116..][0..4], .little);
            entry.stream_size = std.mem.readInt(u64, data[120..][0..8], .little);

            return entry;
        }

        fn getObjectType(self: DirEntry) !ObjectType {
            return switch (self.object_type) {
                0x00 => .unknown,
                0x01 => .storage,
                0x02 => .stream,
                0x05 => .root,
                else => {
                    log.err("invalid CFB object type: 0x{X:0>2} (expected 0x00=unknown, 0x01=storage, 0x02=stream, or 0x05=root)", .{self.object_type});
                    return error.InvalidObjectType;
                },
            };
        }

        fn getName(self: DirEntry, buf: []u8) ![]const u8 {
            // Convert UTF-16LE to UTF-8
            if (self.name_len < 2) return "";
            const char_count = (self.name_len / 2) - 1; // Exclude null terminator

            var len: usize = 0;
            for (0..char_count) |i| {
                const c = self.name[i];
                if (c == 0) break;
                if (c < 0x80) {
                    if (len >= buf.len) return error.BufferTooSmall;
                    buf[len] = @intCast(c);
                    len += 1;
                } else if (c < 0x800) {
                    if (len + 1 >= buf.len) return error.BufferTooSmall;
                    buf[len] = @intCast(0xC0 | (c >> 6));
                    buf[len + 1] = @intCast(0x80 | (c & 0x3F));
                    len += 2;
                } else {
                    if (len + 2 >= buf.len) return error.BufferTooSmall;
                    buf[len] = @intCast(0xE0 | (c >> 12));
                    buf[len + 1] = @intCast(0x80 | ((c >> 6) & 0x3F));
                    buf[len + 2] = @intCast(0x80 | (c & 0x3F));
                    len += 3;
                }
            }
            return buf[0..len];
        }
    };

    fn parse(data: []const u8) !Cfb {
        const header = try Header.parse(data);
        const sector_size = @as(u32, 1) << @intCast(header.sector_shift);
        const mini_sector_size = @as(u32, 1) << @intCast(header.mini_sector_shift);

        return Cfb{
            .content = data,
            .header = header,
            .sector_size = sector_size,
            .mini_sector_size = mini_sector_size,
        };
    }

    fn getSectorData(self: Cfb, sector: u32) ![]const u8 {
        const header_size: u64 = if (self.header.major_version == 3) 512 else self.sector_size;
        const offset = header_size + (@as(u64, sector) * self.sector_size);
        if (offset + self.sector_size > self.content.len) {
            log.err("sector {} out of bounds (offset={}, file_size={})", .{ sector, offset, self.content.len });
            return error.SectorOutOfBounds;
        }
        return self.content[try asIndex(offset)..][0..self.sector_size];
    }

    fn readFat(self: Cfb, allocator: std.mem.Allocator) ![]u32 {
        const entries_per_sector = self.sector_size / 4;
        const total_entries = self.header.num_fat_sectors * entries_per_sector;

        const fat = try allocator.alloc(u32, total_entries);
        errdefer allocator.free(fat);

        var fat_index: usize = 0;

        for (self.header.difat) |sector| {
            if (sector == DirEntry.UNUSED) break;
            if (fat_index >= self.header.num_fat_sectors) break;

            const sector_data = try self.getSectorData(sector);
            for (0..entries_per_sector) |j| {
                const offset = j * 4;
                fat[fat_index * entries_per_sector + j] = std.mem.readInt(u32, sector_data[offset..][0..4], .little);
            }
            fat_index += 1;
        }

        if (self.header.first_difat_sector != DirEntry.UNUSED) {
            var difat_sector = self.header.first_difat_sector;
            var remaining_difat_sectors = self.header.num_difat_sectors;

            while (difat_sector != DirEntry.UNUSED and remaining_difat_sectors > 0) {
                const difat_data = try self.getSectorData(difat_sector);

                const difat_entries_in_sector = (self.sector_size / 4) - 1;
                for (0..difat_entries_in_sector) |i| {
                    if (fat_index >= self.header.num_fat_sectors) break;

                    const sector = std.mem.readInt(u32, difat_data[i * 4 ..][0..4], .little);
                    if (sector == DirEntry.UNUSED) break;

                    const sector_data = try self.getSectorData(sector);
                    for (0..entries_per_sector) |j| {
                        const offset = j * 4;
                        fat[fat_index * entries_per_sector + j] = std.mem.readInt(u32, sector_data[offset..][0..4], .little);
                    }
                    fat_index += 1;
                }

                difat_sector = std.mem.readInt(u32, difat_data[difat_entries_in_sector * 4 ..][0..4], .little);
                remaining_difat_sectors -= 1;
            }
        }

        return fat;
    }

    fn readChain(self: Cfb, fat: []const u32, start_sector: u32, allocator: std.mem.Allocator) ![]u8 {
        var size: usize = 0;
        var sector = start_sector;
        var iterations: usize = 0;
        const max_iterations = fat.len;

        while (sector != DirEntry.END_OF_CHAIN) {
            iterations += 1;
            if (iterations > max_iterations) {
                log.err("FAT chain loop detected at sector {} (start={})", .{ sector, start_sector });
                return error.FatChainLoop;
            }

            size += self.sector_size;
            if (sector >= fat.len) {
                log.err("FAT chain broken: sector {} >= FAT size {}", .{ sector, fat.len });
                return error.InvalidFatChain;
            }

            const next_sector = fat[sector];
            if (next_sector == DirEntry.FAT_SECTOR or
                next_sector == DirEntry.DIFAT_SECTOR or
                (next_sector != DirEntry.END_OF_CHAIN and next_sector >= fat.len))
            {
                log.err("invalid next sector in FAT chain: sector={}, next={}", .{ sector, next_sector });
                return error.InvalidFatChain;
            }
            sector = next_sector;
        }

        const data = try allocator.alloc(u8, size);
        errdefer allocator.free(data);

        var offset: usize = 0;
        sector = start_sector;
        while (sector != DirEntry.END_OF_CHAIN) {
            const sector_data = try self.getSectorData(sector);
            @memcpy(data[offset .. offset + self.sector_size], sector_data);
            offset += self.sector_size;
            sector = fat[sector];
        }

        return data;
    }

    fn readDirEntries(self: Cfb, fat: []const u32, allocator: std.mem.Allocator) ![]DirEntry {
        const dir_data = try self.readChain(fat, self.header.first_dir_sector, allocator);
        defer allocator.free(dir_data);

        const num_entries = dir_data.len / DirEntry.ENTRY_SIZE;
        const entries = try allocator.alloc(DirEntry, num_entries);

        for (0..num_entries) |i| {
            const offset = i * DirEntry.ENTRY_SIZE;
            entries[i] = try DirEntry.parse(dir_data[offset .. offset + DirEntry.ENTRY_SIZE]);
        }

        return entries;
    }

    fn loadStructures(self: *Cfb, allocator: std.mem.Allocator) !void {
        self.fat = try self.readFat(allocator);
        self.dir_entries = try self.readDirEntries(self.fat.?, allocator);
    }

    fn loadMiniFat(self: *Cfb, allocator: std.mem.Allocator) !void {
        if (self.mini_fat != null) return;
        if (self.header.first_mini_fat_sector == DirEntry.UNUSED) return;

        const mini_fat_data = try self.readChain(self.fat.?, self.header.first_mini_fat_sector, allocator);
        defer allocator.free(mini_fat_data);

        const num_entries = mini_fat_data.len / 4;
        self.mini_fat = try allocator.alloc(u32, num_entries);
        for (0..num_entries) |i| {
            const offset = i * 4;
            self.mini_fat.?[i] = std.mem.readInt(u32, mini_fat_data[offset..][0..4], .little);
        }
    }

    fn readStream(self: *Cfb, entry: DirEntry, allocator: std.mem.Allocator) ![]u8 {
        if (entry.stream_size == 0) {
            return try allocator.alloc(u8, 0);
        }

        if (entry.stream_size < self.header.mini_stream_cutoff and try entry.getObjectType() == .stream) {
            try self.loadMiniFat(allocator);

            if (self.mini_stream_data == null) {
                const root_entry = self.dir_entries.?[0];
                if (root_entry.start_sector != DirEntry.UNUSED) {
                    var data = try self.readChain(self.fat.?, root_entry.start_sector, allocator);
                    if (root_entry.stream_size < data.len) {
                        data = try allocator.realloc(data, @intCast(root_entry.stream_size));
                    }
                    self.mini_stream_data = data;
                }
            }
            return try self.readMiniStream(entry, allocator);
        }

        var data = try self.readChain(self.fat.?, entry.start_sector, allocator);

        if (entry.stream_size < data.len) {
            data = try allocator.realloc(data, @intCast(entry.stream_size));
        }

        return data;
    }

    fn readMiniStream(self: Cfb, entry: DirEntry, allocator: std.mem.Allocator) ![]u8 {
        if (self.mini_fat == null or self.mini_stream_data == null) {
            log.err("mini FAT or mini stream not loaded", .{});
            return error.MiniStreamNotLoaded;
        }

        const mini_fat = self.mini_fat.?;
        const mini_stream = self.mini_stream_data.?;

        var size: usize = 0;
        var sector = entry.start_sector;
        while (sector != DirEntry.END_OF_CHAIN) {
            size += self.mini_sector_size;
            if (sector >= mini_fat.len) {
                log.err("mini FAT chain broken: sector {} >= mini FAT size {}", .{ sector, mini_fat.len });
                return error.InvalidMiniFatChain;
            }
            sector = mini_fat[sector];
            if (size > entry.stream_size + self.mini_sector_size) {
                log.err("mini FAT chain too long for stream size {}", .{entry.stream_size});
                return error.InvalidMiniFatChain;
            }
        }

        const data = try allocator.alloc(u8, @intCast(entry.stream_size));
        errdefer allocator.free(data);

        var offset: usize = 0;
        sector = entry.start_sector;
        while (sector != DirEntry.END_OF_CHAIN and offset < entry.stream_size) {
            const mini_sector_offset = @as(usize, sector) * self.mini_sector_size;
            if (mini_sector_offset + self.mini_sector_size > mini_stream.len) {
                log.err("mini sector {} out of bounds", .{sector});
                return error.MiniSectorOutOfBounds;
            }

            const bytes_to_copy = @min(self.mini_sector_size, @as(u32, @intCast(entry.stream_size - offset)));
            @memcpy(data[offset .. offset + bytes_to_copy], mini_stream[mini_sector_offset .. mini_sector_offset + bytes_to_copy]);
            offset += bytes_to_copy;
            sector = mini_fat[sector];
        }

        return data;
    }

    fn findStream(self: Cfb, name: []const u8) ?usize {
        if (self.dir_entries == null) return null;

        var name_buf: [256]u8 = undefined;
        for (self.dir_entries.?, 0..) |entry, i| {
            const entry_name = entry.getName(&name_buf) catch continue;
            if (std.mem.eql(u8, entry_name, name)) {
                return i;
            }
        }
        return null;
    }
};

const MsiDatabase = struct {
    string_pool: []const u8,
    string_data: []const u8,
    string_offsets: []usize,
    file_table: std.StringHashMap(FileEntry),
    directory_table: std.StringHashMap(DirectoryEntry),
    component_table: std.StringHashMap(ComponentEntry),
    media_table: std.StringHashMap(MediaEntry),
    allocator: std.mem.Allocator,
    table_columns: std.StringHashMap(ArrayList(ColumnInfo)),
    resolved_paths_cache: std.StringHashMap([]const u8),

    const MsiColumnType = packed struct(i16) {
        width: u8, // Bits 0-7: For strings/ints: width/size in bytes
        valid: bool, // Bit 8: 0x0100
        localizable: bool, // Bit 9: 0x0200
        _unused: bool = false, // Bit 10: 0x0400 (unused)
        string: bool, // Bit 11: 0x0800
        nullable: bool, // Bit 12: 0x1000
        key: bool, // Bit 13: 0x2000
        temporary: bool, // Bit 14: 0x4000
        unknown: bool, // Bit 15: 0x8000

        fn getSize(self: MsiColumnType, bytes_per_strref: usize) usize {
            const base_type = self.width & 0xFF;
            return switch (base_type) {
                0, 1, 2 => bytes_per_strref,
                3 => 2,
                4 => 4,
                5 => 0,
                else => bytes_per_strref,
            };
        }
    };

    const ColumnInfo = struct {
        name: []const u8,
        col_type: MsiColumnType,

        fn getSize(self: ColumnInfo, bytes_per_strref: usize) usize {
            return self.col_type.getSize(bytes_per_strref);
        }
    };

    const FileEntry = struct {
        file_id: []const u8,
        file_name: []const u8,
        file_size: u32,
        component: []const u8,
    };

    const DirectoryEntry = struct {
        directory: []const u8,
        directory_parent: []const u8,
        default_dir: []const u8,
    };

    const ComponentEntry = struct {
        component: []const u8,
        directory: []const u8,
    };

    const MediaEntry = struct {
        disk_id: u16,
        cabinet: []const u8,
    };

    fn getBytesPerStrRef(self: *const MsiDatabase) u32 {
        if (self.string_pool.len < 4) return 2;

        const pool1 = std.mem.readInt(u16, self.string_pool[2..][0..2], .little);
        if ((pool1 & 0x8000) != 0) {
            return 3;
        }
        return 2;
    }

    fn mimeToChar(value: u8) u8 {
        return switch (value) {
            0...9 => '0' + value,
            10...35 => 'A' + (value - 10),
            36...61 => 'a' + (value - 36),
            62 => '.',
            else => '_',
        };
    }

    fn decodeStreamName(encoded_name: []const u16, buf: []u8) ![]const u8 {
        var out_idx: usize = 0;
        var i: usize = 0;

        while (i < encoded_name.len) : (i += 1) {
            const c = encoded_name[i];

            if (c == 0) break;

            if (i == 0 and (c == 0x4840 or c == 0x5)) continue;

            if (c >= 0x3800 and c < 0x4800) {
                if (out_idx < buf.len) {
                    buf[out_idx] = mimeToChar(@intCast(c & 0x3f));
                    out_idx += 1;
                }
                if (out_idx < buf.len) {
                    buf[out_idx] = mimeToChar(@intCast((c >> 6) & 0x3f));
                    out_idx += 1;
                }
            } else if (c >= 0x4800 and c < 0x4840) {
                if (out_idx < buf.len) {
                    buf[out_idx] = mimeToChar(@intCast(c - 0x4800));
                    out_idx += 1;
                }
            } else if (c < 0x80) {
                if (out_idx < buf.len) {
                    buf[out_idx] = @intCast(c);
                    out_idx += 1;
                }
            }
        }

        return buf[0..out_idx];
    }

    fn parse(cfb: *Cfb, allocator: std.mem.Allocator) !MsiDatabase {
        var db = MsiDatabase{
            .string_pool = &[_]u8{},
            .string_data = &[_]u8{},
            .string_offsets = &[_]usize{},
            .file_table = std.StringHashMap(FileEntry).init(allocator),
            .directory_table = std.StringHashMap(DirectoryEntry).init(allocator),
            .component_table = std.StringHashMap(ComponentEntry).init(allocator),
            .media_table = std.StringHashMap(MediaEntry).init(allocator),
            .table_columns = std.StringHashMap(ArrayList(ColumnInfo)).init(allocator),
            .resolved_paths_cache = std.StringHashMap([]const u8).init(allocator),
            .allocator = allocator,
        };
        errdefer db.deinit();

        var string_pool_idx: ?usize = null;
        var string_data_idx: ?usize = null;
        var file_table_idx: ?usize = null;
        var directory_table_idx: ?usize = null;
        var component_table_idx: ?usize = null;
        var media_table_idx: ?usize = null;
        var columns_table_idx: ?usize = null;

        var name_buf: [256]u8 = undefined;
        for (cfb.dir_entries.?, 0..) |entry, idx| {
            if (entry.object_type == 0) continue;
            if (try entry.getObjectType() != .stream) continue;

            const decoded = decodeStreamName(entry.name[0..], &name_buf) catch |err| {
                log.warn("failed to decode stream name at index {}: {}", .{ idx, err });
                return error.UnknownStreamName;
            };

            if (decoded.len > 0) {
                // log.info("Stream {}: '{s}'", .{ idx, decoded });
                if (std.mem.indexOf(u8, decoded, "ytLiHgvoIl") != null or
                    std.mem.eql(u8, decoded, "_StringPool"))
                {
                    string_pool_idx = idx;
                } else if (std.mem.indexOf(u8, decoded, "ytLiHgjaNa") != null or
                    std.mem.eql(u8, decoded, "_StringData"))
                {
                    string_data_idx = idx;
                } else if (std.mem.indexOf(u8, decoded, "Cl8") != null or
                    std.mem.eql(u8, decoded, "File"))
                {
                    file_table_idx = idx;
                } else if (std.mem.indexOf(u8, decoded, "Cr8cNoLy") != null or
                    std.mem.eql(u8, decoded, "Directory"))
                {
                    directory_table_idx = idx;
                } else if (std.mem.indexOf(u8, decoded, "ImJoHeHt") != null or
                    std.mem.eql(u8, decoded, "Component"))
                {
                    component_table_idx = idx;
                } else if (std.mem.indexOf(u8, decoded, "M8dCa") != null or
                    std.mem.eql(u8, decoded, "Media"))
                {
                    media_table_idx = idx;
                } else if (std.mem.indexOf(u8, decoded, "ioFuGn") != null or
                    std.mem.eql(u8, decoded, "_Columns"))
                {
                    columns_table_idx = idx;
                }
            }
        }
        if (string_pool_idx) |idx| {
            const entry = cfb.dir_entries.?[idx];
            db.string_pool = try cfb.readStream(entry, allocator);
        } else {
            log.err("MSI database missing required _StringPool table", .{});
            return error.MissingStringPool;
        }

        if (string_data_idx) |idx| {
            const entry = cfb.dir_entries.?[idx];
            db.string_data = try cfb.readStream(entry, allocator);
        } else {
            log.err("MSI database missing required _StringData table", .{});
            return error.MissingStringData;
        }

        if (db.string_pool.len > 0) {
            try db.buildStringOffsets();
        }

        if (columns_table_idx) |idx| {
            try db.parseColumnsTable(cfb, idx);
        } else {
            log.err("MSI database missing required _Columns table", .{});
            return error.MissingColumnsTable;
        }

        if (file_table_idx) |idx| {
            try db.parseFileTable(cfb, idx);
        } else {
            log.err("MSI database missing required File table", .{});
            return error.MissingFileTable;
        }

        if (directory_table_idx) |idx| {
            try db.parseDirectoryTable(cfb, idx);
        }
        if (component_table_idx) |idx| {
            try db.parseComponentTable(cfb, idx);
        }
        if (media_table_idx) |idx| {
            try db.parseMediaTable(cfb, idx);
        }
        return db;
    }

    fn buildStringOffsets(self: *MsiDatabase) !void {
        const header_size = 4;
        const entry_size = 4;

        if (self.string_pool.len < header_size) return;

        const num_entries = (self.string_pool.len - header_size) / entry_size;
        if (num_entries == 0) return;

        self.string_offsets = try self.allocator.alloc(usize, num_entries);

        var data_offset: usize = 0;
        var i: usize = 0;
        while (i < num_entries) {
            const pool_offset = header_size + i * entry_size;
            if (pool_offset + entry_size > self.string_pool.len) break;

            const length = std.mem.readInt(u16, self.string_pool[pool_offset..][0..2], .little);
            const refcount = std.mem.readInt(u16, self.string_pool[pool_offset + 2 ..][0..2], .little);

            self.string_offsets[i] = data_offset;

            if (length == 0 and refcount != 0 and pool_offset + entry_size * 2 <= self.string_pool.len) {
                const high_word = refcount;
                const low_word = std.mem.readInt(u16, self.string_pool[pool_offset + entry_size ..][0..2], .little);
                const long_len: usize = (@as(usize, high_word) << 16) | low_word;
                data_offset += long_len;
                i += 1;
                if (i < num_entries) {
                    self.string_offsets[i] = data_offset;
                }
            } else {
                data_offset += length;
            }

            i += 1;
        }
    }

    fn getString(self: *const MsiDatabase, string_id: u32) ![]const u8 {
        if (string_id == 0) return "";

        const array_index = string_id - 1;
        if (array_index >= self.string_offsets.len) {
            log.err("invalid string ID: {} (valid range: 1-{}, array index: {} >= {})", .{ string_id, self.string_offsets.len, array_index, self.string_offsets.len });
            return error.StringIdOutOfRange;
        }

        const data_offset = self.string_offsets[array_index];

        const header_size = 4;
        const entry_size = 4;
        const pool_entry_offset = header_size + array_index * entry_size;

        if (pool_entry_offset + entry_size > self.string_pool.len) {
            log.err("string pool entry out of bounds: string_id={}, pool_entry_offset={}, entry_size={}, string_pool.len={}", .{ string_id, pool_entry_offset, entry_size, self.string_pool.len });
            return error.StringIdOutOfRange;
        }

        const length = std.mem.readInt(u16, self.string_pool[pool_entry_offset..][0..2], .little);

        if (data_offset + length > self.string_data.len) {
            log.err("string data out of bounds: string_id={}, data_offset={}, length={}, string_data.len={}", .{ string_id, data_offset, length, self.string_data.len });
            return error.StringDataOutOfRange;
        }

        return self.string_data[data_offset .. data_offset + length];
    }

    fn readStringRef(data: []const u8, offset: usize, bytes_per_ref: u32) !u32 {
        return switch (bytes_per_ref) {
            2 => std.mem.readInt(u16, data[offset..][0..2], .little),
            3 => blk: {
                const b0: u32 = data[offset];
                const b1: u32 = data[offset + 1];
                const b2: u32 = data[offset + 2];
                break :blk b0 | (b1 << 8) | (b2 << 16);
            },
            4 => std.mem.readInt(u32, data[offset..][0..4], .little),
            else => error.InvalidBytesPerRef,
        };
    }

    fn parseColumnsTable(self: *MsiDatabase, cfb: *Cfb, stream_idx: usize) !void {
        const entry = cfb.dir_entries.?[stream_idx];
        const table_data = try cfb.readStream(entry, self.allocator);
        defer self.allocator.free(table_data);

        const bytes_per_strref = self.getBytesPerStrRef();

        const col1_size = bytes_per_strref;
        const col2_size = 2;
        const col3_size = bytes_per_strref;
        const col4_size = 2;

        const total_col_size = col1_size + col2_size + col3_size + col4_size;
        const num_rows = table_data.len / total_col_size;

        if (table_data.len % total_col_size != 0) {
            log.err("_Columns table size mismatch: {} bytes not evenly divisible by {} bytes/row", .{ table_data.len, total_col_size });
            return error.InvalidColumnsTable;
        }

        var col_offset: usize = 0;
        const col1_offset = col_offset;
        col_offset += col1_size * num_rows;
        const col2_offset = col_offset;
        col_offset += col2_size * num_rows;
        const col3_offset = col_offset;
        col_offset += col3_size * num_rows;
        const col4_offset = col_offset;

        var row: usize = 0;
        while (row < num_rows) : (row += 1) {
            const table_id = try readStringRef(table_data, col1_offset + row * col1_size, bytes_per_strref);
            const table_name = self.getString(table_id) catch |err| {
                log.err("_Columns row {}: Failed to get table name for id {}: {}", .{ row, table_id, err });
                return err;
            };

            const col_number = std.mem.readInt(i16, table_data[col2_offset + row * col2_size ..][0..2], .little);
            if (col_number > 0 or col_number < -50000) {
                log.warn("_Columns row {}: unusual column number {} for table '{s}'", .{ row, col_number, table_name });
                return error.InvalidColNumber;
            }

            const name_id = try readStringRef(table_data, col3_offset + row * col3_size, bytes_per_strref);
            const col_name = self.getString(name_id) catch |err| {
                log.err("_Columns row {}: Failed to get column name for table '{s}': {}", .{ row, table_name, err });
                return err;
            };

            const col_type_raw = std.mem.readInt(i16, table_data[col4_offset + row * col4_size ..][0..2], .little);
            const col_type: MsiColumnType = @bitCast(col_type_raw);

            const gop = try self.table_columns.getOrPut(table_name);
            if (!gop.found_existing) {
                gop.value_ptr.* = try ArrayList(ColumnInfo).initCapacity(self.allocator, 8);
            }

            try gop.value_ptr.append(self.allocator, ColumnInfo{
                .name = col_name,
                .col_type = col_type,
            });
        }
    }

    fn parseFileTable(self: *MsiDatabase, cfb: *Cfb, stream_idx: usize) !void {
        const entry = cfb.dir_entries.?[stream_idx];
        const table_data = try cfb.readStream(entry, self.allocator);
        defer self.allocator.free(table_data);

        const bytes_per_strref = self.getBytesPerStrRef();

        const columns = self.table_columns.get("File") orelse {
            log.err("File table not found in _Columns table", .{});
            return error.MissingTableSchema;
        };

        var total_col_size: usize = 0;
        for (columns.items) |col| {
            total_col_size += col.getSize(bytes_per_strref);
        }

        var data_offset: usize = 0;
        var num_rows: usize = 0;
        if (table_data.len >= 4) {
            const potential_row_count = std.mem.readInt(u32, table_data[0..4], .little);
            const expected_size_with_header = 4 + potential_row_count * total_col_size;
            if (expected_size_with_header == table_data.len) {
                num_rows = potential_row_count;
                data_offset = 4;
            } else {
                num_rows = table_data.len / total_col_size;
            }
        } else {
            num_rows = table_data.len / total_col_size;
        }

        if (data_offset + num_rows * total_col_size != table_data.len) {
            const expected = data_offset + num_rows * total_col_size;
            const actual = table_data.len;
            const diff = if (actual > expected) actual - expected else expected - actual;
            log.err("File table size mismatch!", .{});
            log.err("  Expected: {} bytes ({} offset + {} rows * {} bytes/row)", .{ expected, data_offset, num_rows, total_col_size });
            log.err("  Actual: {} bytes", .{actual});
            log.err("  Difference: {} bytes", .{diff});
            log.err("Columns: ", .{});
            for (columns.items, 0..) |col, i| {
                log.err("  [{}] {s}: type={} size={}", .{ i, col.name, col.col_type, col.getSize(bytes_per_strref) });
            }
            return error.TableSizeMismatch;
        }

        if (num_rows == 0) return;

        var file_col: ?struct { idx: usize, offset: usize, size: usize } = null;
        var component_col: ?struct { idx: usize, offset: usize, size: usize } = null;
        var filename_col: ?struct { idx: usize, offset: usize, size: usize } = null;
        var filesize_col: ?struct { idx: usize, offset: usize, size: usize } = null;

        var col_offset: usize = data_offset;
        for (columns.items, 0..) |col, idx| {
            const col_size = col.getSize(bytes_per_strref);
            if (std.mem.eql(u8, col.name, "File")) {
                file_col = .{ .idx = idx, .offset = col_offset, .size = col_size };
            } else if (std.mem.eql(u8, col.name, "Component_")) {
                component_col = .{ .idx = idx, .offset = col_offset, .size = col_size };
            } else if (std.mem.eql(u8, col.name, "FileName")) {
                filename_col = .{ .idx = idx, .offset = col_offset, .size = col_size };
            } else if (std.mem.eql(u8, col.name, "FileSize")) {
                filesize_col = .{ .idx = idx, .offset = col_offset, .size = col_size };
            }
            col_offset += col_size * num_rows;
        }

        var row: usize = 0;
        while (row < num_rows) : (row += 1) {
            var file_id_str: []const u8 = "";
            var component_str: []const u8 = "";
            var filename_str: []const u8 = "";
            var file_size: u32 = 0;

            if (file_col) |col_info| {
                const id = try readStringRef(table_data, col_info.offset + row * col_info.size, bytes_per_strref);
                file_id_str = self.getString(id) catch |err| {
                    log.warn("File table row {}: Failed to get File string {}: {}", .{ row, id, err });
                    continue;
                };
            }

            if (component_col) |col_info| {
                const id = try readStringRef(table_data, col_info.offset + row * col_info.size, bytes_per_strref);
                component_str = self.getString(id) catch |err| {
                    log.err("File table row {}: Failed to get Component_ string {}: {}", .{ row, id, err });
                    return err;
                };
            }

            if (filename_col) |col_info| {
                const id = try readStringRef(table_data, col_info.offset + row * col_info.size, bytes_per_strref);
                filename_str = self.getString(id) catch |err| {
                    log.err("File table row {}: Failed to get FileName string {}: {}", .{ row, id, err });
                    return err;
                };
            }

            if (filesize_col) |col_info| {
                file_size = std.mem.readInt(u32, table_data[col_info.offset + row * col_info.size ..][0..4], .little);
            }

            if (file_id_str.len == 0) continue;

            try self.file_table.put(file_id_str, FileEntry{
                .file_id = file_id_str,
                .file_name = filename_str,
                .file_size = file_size,
                .component = component_str,
            });
        }
    }

    fn parseDirectoryTable(self: *MsiDatabase, cfb: *Cfb, stream_idx: usize) !void {
        const entry = cfb.dir_entries.?[stream_idx];
        const table_data = try cfb.readStream(entry, self.allocator);
        defer self.allocator.free(table_data);

        const bytes_per_strref = self.getBytesPerStrRef();

        const col1_size = bytes_per_strref;
        const col2_size = bytes_per_strref;
        const col3_size = bytes_per_strref;

        const total_col_size = col1_size + col2_size + col3_size;
        const num_rows = table_data.len / total_col_size;

        if (num_rows == 0) return;

        var col_offset: usize = 0;
        const col1_offset = col_offset;
        col_offset += col1_size * num_rows;
        const col2_offset = col_offset;
        col_offset += col2_size * num_rows;
        const col3_offset = col_offset;

        var row: usize = 0;
        while (row < num_rows) : (row += 1) {
            const dir_id = try readStringRef(table_data, col1_offset + row * col1_size, bytes_per_strref);
            const dir_id_str = self.getString(dir_id) catch |err| {
                log.err("Directory table row {}: Failed to get directory ID string {}: {}", .{ row, dir_id, err });
                return err;
            };

            const parent_id = try readStringRef(table_data, col2_offset + row * col2_size, bytes_per_strref);
            const parent_str = if (parent_id == 0) "" else self.getString(parent_id) catch |err| {
                log.err("Directory table row {}: Failed to get parent string {} for directory '{s}': {}", .{ row, parent_id, dir_id_str, err });
                return err;
            };

            const default_dir_id = try readStringRef(table_data, col3_offset + row * col3_size, bytes_per_strref);
            const default_dir_str = self.getString(default_dir_id) catch |err| {
                log.err("Directory table row {}: Failed to get default_dir string {} for directory '{s}': {}", .{ row, default_dir_id, dir_id_str, err });
                return err;
            };

            try self.directory_table.put(dir_id_str, DirectoryEntry{
                .directory = dir_id_str,
                .directory_parent = parent_str,
                .default_dir = default_dir_str,
            });
        }
    }

    fn parseComponentTable(self: *MsiDatabase, cfb: *Cfb, stream_idx: usize) !void {
        const entry = cfb.dir_entries.?[stream_idx];
        const table_data = try cfb.readStream(entry, self.allocator);
        defer self.allocator.free(table_data);

        const bytes_per_strref = self.getBytesPerStrRef();

        const col1_size = bytes_per_strref;
        const col2_size = bytes_per_strref;
        const col3_size = bytes_per_strref;
        const col4_size = 2;
        const col5_size = bytes_per_strref;
        const col6_size = bytes_per_strref;

        const total_col_size = col1_size + col2_size + col3_size + col4_size + col5_size + col6_size;
        const num_rows = table_data.len / total_col_size;

        if (num_rows == 0) return;

        var col_offset: usize = 0;
        const col1_offset = col_offset;
        col_offset += col1_size * num_rows;
        col_offset += col2_size * num_rows;
        const col3_offset = col_offset;

        var row: usize = 0;
        while (row < num_rows) : (row += 1) {
            const comp_id = try readStringRef(table_data, col1_offset + row * col1_size, bytes_per_strref);
            const comp_id_str = self.getString(comp_id) catch |err| {
                log.err("Component table row {}: Failed to get component ID string {}: {}", .{ row, comp_id, err });
                return err;
            };

            const dir_id = try readStringRef(table_data, col3_offset + row * col3_size, bytes_per_strref);
            const dir_str = self.getString(dir_id) catch |err| {
                log.err("Component table row {}: Failed to get directory string {} for component '{s}': {}", .{ row, dir_id, comp_id_str, err });
                return err;
            };

            try self.component_table.put(comp_id_str, ComponentEntry{
                .component = comp_id_str,
                .directory = dir_str,
            });
        }
    }

    fn parseMediaTable(self: *MsiDatabase, cfb: *Cfb, stream_idx: usize) !void {
        const entry = cfb.dir_entries.?[stream_idx];
        const table_data = try cfb.readStream(entry, self.allocator);
        defer self.allocator.free(table_data);

        const bytes_per_strref = self.getBytesPerStrRef();

        const columns = self.table_columns.get("Media") orelse {
            log.warn("Media table schema not found in _Columns table, skipping", .{});
            return;
        };

        var total_col_size: usize = 0;
        for (columns.items) |col| {
            total_col_size += col.getSize(bytes_per_strref);
        }

        var data_offset: usize = 0;
        var num_rows: usize = 0;
        if (table_data.len >= 4) {
            const potential_row_count = std.mem.readInt(u32, table_data[0..4], .little);
            const expected_size_with_header = 4 + potential_row_count * total_col_size;
            if (expected_size_with_header == table_data.len) {
                num_rows = potential_row_count;
                data_offset = 4;
            } else {
                num_rows = table_data.len / total_col_size;
            }
        } else {
            num_rows = table_data.len / total_col_size;
        }

        if (num_rows == 0) return;

        var diskid_col: ?struct { idx: usize, offset: usize, size: usize } = null;
        var cabinet_col: ?struct { idx: usize, offset: usize, size: usize } = null;

        var col_offset: usize = data_offset;
        for (columns.items, 0..) |col, idx| {
            const col_size = col.getSize(bytes_per_strref);
            if (std.mem.eql(u8, col.name, "DiskId")) {
                diskid_col = .{ .idx = idx, .offset = col_offset, .size = col_size };
            } else if (std.mem.eql(u8, col.name, "Cabinet")) {
                cabinet_col = .{ .idx = idx, .offset = col_offset, .size = col_size };
            }
            col_offset += col_size * num_rows;
        }

        if (diskid_col == null) {
            log.warn("Media table missing DiskId column", .{});
            return;
        }

        var row: usize = 0;
        while (row < num_rows) : (row += 1) {
            const disk_id = std.mem.readInt(u16, table_data[diskid_col.?.offset + row * diskid_col.?.size ..][0..2], .little);

            var cabinet_str: []const u8 = "";
            if (cabinet_col) |col_info| {
                const cabinet_id = try readStringRef(table_data, col_info.offset + row * col_info.size, bytes_per_strref);
                cabinet_str = if (cabinet_id == 0) "" else self.getString(cabinet_id) catch |err| {
                    log.err("Media table row {}: Failed to get Cabinet string {}: {}", .{ row, cabinet_id, err });
                    return err;
                };
            }

            if (cabinet_str.len > 0) {
                var key_buf: [16]u8 = undefined;
                const key = std.fmt.bufPrint(&key_buf, "{}", .{disk_id}) catch unreachable;
                const key_copy = try self.allocator.dupe(u8, key);
                try self.media_table.put(key_copy, MediaEntry{
                    .disk_id = disk_id,
                    .cabinet = cabinet_str,
                });
            }
        }
    }

    fn resolveDirectoryPath(self: *MsiDatabase, dir_id: []const u8, allocator: std.mem.Allocator) ![]const u8 {
        if (self.resolved_paths_cache.get(dir_id)) |cached_path| {
            return cached_path;
        }

        var path_parts: std.ArrayListUnmanaged([]const u8) = .{};
        defer path_parts.deinit(allocator);

        var current_dir_id = dir_id;
        var visited = std.StringHashMap(void).init(allocator);
        defer visited.deinit();

        while (true) {
            if (visited.contains(current_dir_id)) break;
            try visited.put(current_dir_id, {});

            const dir_entry = self.directory_table.get(current_dir_id) orelse {
                log.warn("Directory '{s}' not found in Directory table", .{current_dir_id});
                break;
            };

            const dir_name = blk: {
                if (std.mem.indexOf(u8, dir_entry.default_dir, "|")) |pipe_pos| {
                    break :blk dir_entry.default_dir[pipe_pos + 1 ..];
                } else if (std.mem.indexOf(u8, dir_entry.default_dir, ":")) |colon_pos| {
                    break :blk dir_entry.default_dir[colon_pos + 1 ..];
                } else {
                    break :blk dir_entry.default_dir;
                }
            };

            if (!std.mem.eql(u8, dir_name, ".") and !std.mem.eql(u8, dir_name, "SourceDir")) {
                try path_parts.append(allocator, dir_name);
            }

            if (dir_entry.directory_parent.len == 0) break;
            current_dir_id = dir_entry.directory_parent;
        }

        std.mem.reverse([]const u8, path_parts.items);

        const resolved_path = try std.mem.join(allocator, std.fs.path.sep_str, path_parts.items);

        const dir_id_copy = try allocator.dupe(u8, dir_id);
        errdefer allocator.free(dir_id_copy);
        try self.resolved_paths_cache.put(dir_id_copy, resolved_path);

        return resolved_path;
    }

    fn deinit(self: *MsiDatabase) void {
        if (self.string_pool.len > 0) self.allocator.free(self.string_pool);
        if (self.string_data.len > 0) self.allocator.free(self.string_data);
        if (self.string_offsets.len > 0) self.allocator.free(self.string_offsets);
        self.file_table.deinit();
        self.directory_table.deinit();
        self.component_table.deinit();

        var media_iter = self.media_table.iterator();
        while (media_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.media_table.deinit();

        var iter = self.table_columns.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.table_columns.deinit();

        var cache_iter = self.resolved_paths_cache.iterator();
        while (cache_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.resolved_paths_cache.deinit();
    }
};

fn asIndex(index: u64) error{IndexOutOfRange}!usize {
    if (@sizeOf(usize) == @sizeOf(u64)) return index;
    return std.math.cast(usize, index) orelse {
        std.log.err("u64 index {} exceeds max usize", .{index});
        return error.IndexOutOfRange;
    };
}

const zig15 = @import("builtin").zig_version.order(.{ .major = 0, .minor = 15, .patch = 0 }) != .lt;
const std15 = if (zig15) std else @import("std15");
const vers = @import("vers");

const log = std.log.scoped(.msi);

const std = @import("std");
const stdfork = @import("stdfork");
const ArrayList = if (zig15) std.ArrayList else std.ArrayListUnmanaged;
