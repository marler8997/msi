pub const Sha = struct {
    bytes: [32]u8,
    pub fn parseHex(hex: *const [64]u8) ?Sha {
        var result: Sha = undefined;
        for (&result.bytes, 0..) |*byte, i| {
            const high: u8 = nibbleFromHex(hex[i * 2]) orelse return null;
            const low: u8 = nibbleFromHex(hex[i * 2 + 1]) orelse return null;
            byte.* = (high << 4) | low;
        }
        return result;
    }
    fn nibbleFromHex(c: u8) ?u4 {
        return switch (c) {
            '0'...'9' => @intCast(c - '0'),
            'a'...'f' => @intCast(c - 'a' + 10),
            else => null,
        };
    }
    pub fn eql(sha: *const Sha, other: *const Sha) bool {
        return std.mem.eql(u8, &sha.bytes, &other.bytes);
    }
    pub fn toHex(sha: *const Sha) [64]u8 {
        var buf: [64]u8 = undefined;
        std.debug.assert(64 == (std.fmt.bufPrint(&buf, "{f}", .{sha}) catch unreachable).len);
        return buf;
    }
    pub const format = if (zig15) formatNew else formatOld;
    pub fn formatNew(value: Sha, writer: *std.Io.Writer) error{WriteFailed}!void {
        try writer.print("{x}", .{&value.bytes});
    }
    pub fn formatOld(value: Sha, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;
        try writer.print("{}", .{std.fmt.fmtSliceHexLower(&value.bytes)});
    }
};

pub const File = struct {
    path: []const u8,
    content: []const u8,

    pub fn read(allocator: std.mem.Allocator, path: []const u8) !File {
        return .{
            .path = path,
            .content = blk: {
                const file = try std.fs.cwd().openFile(path, .{});
                defer file.close();
                break :blk try file.readToEndAlloc(allocator, std.math.maxInt(usize));
            },
        };
    }
    pub fn deinit(file: *const File, allocator: std.mem.Allocator) void {
        allocator.free(file.content);
    }
    pub fn iterator(file: *const File) Iterator {
        return .{
            .filename = file.path,
            .lineno = 0,
            .line_it = std.mem.splitScalar(u8, file.content, '\n'),
        };
    }

    pub fn fetchAll(
        file: *const File,
        scratch: std.mem.Allocator,
        cache_path: []const u8,
        scratch_path: []const u8,
    ) !void {
        var it = file.iterator();
        while (it.next()) |entry| {
            const basename = basenameFromUri(entry.uri);
            const cache_basename = try std.fmt.allocPrint(scratch, "{f}-{s}", .{ entry.sha, basename });
            defer scratch.free(cache_basename);
            const file_cache_path = try std.fs.path.join(scratch, &.{ cache_path, cache_basename });
            defer scratch.free(file_cache_path);

            try fetch(scratch, entry.uri, file_cache_path);
            const installer_path = try std.fs.path.join(scratch, &.{ scratch_path, "installer", basename });
            defer scratch.free(installer_path);
            try std.fs.cwd().makePath(std.fs.path.dirname(installer_path).?);
            try std.fs.cwd().copyFile(file_cache_path, std.fs.cwd(), installer_path, .{});
        }
    }
};

pub const Entry = struct {
    kind: Kind,
    uri: []const u8,
    sha: Sha,

    pub const Kind = enum { cab, msi };
};

pub const Iterator = struct {
    filename: []const u8,
    lineno: u32,
    line_it: std.mem.SplitIterator(u8, .scalar),

    pub fn next(it: *Iterator) ?Entry {
        const uri_string = nextLine(&it.line_it, &it.lineno) orelse return null;
        const uri: std.Uri = std.Uri.parse(uri_string) catch errExit("{s}:{}: invalid uri", .{ it.filename, it.lineno });
        if (uri.path.isEmpty()) errExit("{s}:{}: uri missing path", .{ it.filename, it.lineno });
        const kind: Entry.Kind = blk: {
            if (std.mem.endsWith(u8, uri_string, ".cab")) break :blk .cab;
            if (std.mem.endsWith(u8, uri_string, ".msi")) break :blk .msi;
            errExit("{s}:{}: URI does not end with .cab nor .msi", .{ it.filename, it.lineno });
        };
        const sha: Sha = blk: {
            const line = nextLine(&it.line_it, &it.lineno) orelse errExit("{s}:{}: missing hash", .{ it.filename, it.lineno });
            if (line.len == 64) {
                if (Sha.parseHex(line[0..64])) |sha| break :blk sha;
            }
            errExit("{s}:{}: invalid hash", .{ it.filename, it.lineno });
        };
        return .{ .kind = kind, .uri = uri_string, .sha = sha };
    }
};
fn nextLine(it: *std.mem.SplitIterator(u8, .scalar), lineno: *u32) ?[]const u8 {
    while (true) {
        const full = it.next() orelse return null;
        lineno.* += 1;
        const trimmed = std.mem.trim(u8, full, " \r\n");
        if (trimmed.len == 0) continue;
        if (trimmed[0] == '#') continue;
        return trimmed;
    }
}

fn fetch(scratch: std.mem.Allocator, uri: []const u8, out: []const u8) !void {
    if (std.fs.path.dirname(out)) |d| try std.fs.cwd().makePath(d);

    const lock_path = try std.mem.concat(scratch, u8, &.{ out, ".lock" });
    defer scratch.free(lock_path);
    var lock = try LockFile.lock(lock_path);
    defer lock.unlock();

    if (std.fs.cwd().access(out, .{})) {
        std.log.info("{s}: already fetched", .{out});
        return;
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => |e| return e,
    }

    const encoded_uri = try uriEncode(scratch, uri);
    defer if (encoded_uri.ptr != uri.ptr) scratch.free(encoded_uri);

    const argv = [_][]const u8{
        "curl", "--fail", "-L", "-o", out, encoded_uri,
    };
    {
        var stderr_buf: [1000]u8 = undefined;
        var stderr: File15.Writer = .init(stderrFile(), &stderr_buf);
        flushRun(&stderr.interface, &argv) catch return stderr.err.?;
    }

    var proc_arena = std.heap.ArenaAllocator.init(scratch);
    defer proc_arena.deinit();
    const result = try std.process.Child.run(.{
        .allocator = proc_arena.allocator(),
        .argv = &argv,
    });
    if (switch (result.term) {
        .Exited => |code| code != 0,
        else => true,
    }) {
        var stderr: File15.Writer = .init(stderrFile(), &.{});
        stderr.interface.writeAll(result.stdout) catch return stderr.err.?;
        stderr.interface.writeAll(result.stderr) catch return stderr.err.?;
    }
    switch (result.term) {
        .Exited => |code| if (code != 0) errExit("curl exited with code {}", .{code}),
        inline else => |sig, kind| errExit("curl stopped ({s}) with {}", .{ @tagName(kind), sig }),
    }
}

pub fn flushRun(writer: *std15.Io.Writer, cli: []const []const u8) error{WriteFailed}!void {
    var prefix: []const u8 = "";
    for (cli) |a| {
        const do_quote = std.mem.indexOfScalar(u8, a, ' ') != null;
        const quote: []const u8 = if (do_quote) "\"" else "";
        try writer.print("{s}{s}{s}{1s}", .{ prefix, quote, a });
        prefix = " ";
    }
    try writer.writeAll("\n");
    try writer.flush();
}

fn uriEncode(arena: std.mem.Allocator, raw: []const u8) ![]const u8 {
    var len: usize = 0;
    for (raw) |c| {
        len += if (c == ' ') @as(usize, 3) else 1;
    }
    if (len == raw.len) return raw; // no encoding needed

    const buf = try arena.alloc(u8, len);
    var i: usize = 0;
    for (raw) |c| {
        if (c == ' ') {
            buf[i] = '%';
            buf[i + 1] = '2';
            buf[i + 2] = '0';
            i += 3;
        } else {
            buf[i] = c;
            i += 1;
        }
    }
    return buf;
}

pub fn allocTree(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    var tree: std15.Io.Writer.Allocating = try .initCapacity(allocator, 4096);
    defer tree.deinit();
    writeTree(&tree.writer, allocator, path) catch |err| switch (err) {
        error.WriteFailed => return error.OutOfMemory,
        else => |e| return e,
    };
    return tree.toOwnedSlice();
}

fn writeTree(writer: *std15.Io.Writer, allocator: std.mem.Allocator, install_dir: []const u8) !void {
    // Collect all output lines
    var lines = try ArrayList([]const u8).initCapacity(allocator, 1024);
    defer {
        for (lines.items) |line| allocator.free(line);
        lines.deinit(allocator);
    }

    // Walk directory tree
    var dir = try std.fs.cwd().openDir(install_dir, .{ .iterate = true });
    defer dir.close();

    try walkDirectory(allocator, dir, "", &lines);

    // Sort all lines for reproducibility
    std.mem.sort([]const u8, lines.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lessThan);

    // Write sorted output
    for (lines.items) |line| {
        try writer.writeAll(line);
        try writer.writeAll("\n");
    }
    try writer.flush();
}

fn walkDirectory(allocator: std.mem.Allocator, dir: std.fs.Dir, rel_path: []const u8, lines: *ArrayList([]const u8)) !void {
    var iter = dir.iterate();
    var entries = try ArrayList(std.fs.Dir.Entry).initCapacity(allocator, 32);
    defer entries.deinit(allocator);

    // Collect all entries
    while (try iter.next()) |entry| {
        const name_copy = try allocator.dupe(u8, entry.name);
        try entries.append(allocator, .{ .name = name_copy, .kind = entry.kind });
    }

    // Sort for deterministic output
    std.mem.sort(std.fs.Dir.Entry, entries.items, {}, struct {
        fn lessThan(_: void, a: std.fs.Dir.Entry, b: std.fs.Dir.Entry) bool {
            return std.mem.lessThan(u8, a.name, b.name);
        }
    }.lessThan);

    for (entries.items) |entry| {
        defer allocator.free(entry.name);

        const full_path = if (rel_path.len > 0)
            try std.fmt.allocPrint(allocator, "{s}/{s}", .{ rel_path, entry.name })
        else
            try allocator.dupe(u8, entry.name);
        defer allocator.free(full_path);

        switch (entry.kind) {
            .file => {
                // Calculate SHA256 hash
                const file = try dir.openFile(entry.name, .{});
                defer file.close();

                var hasher = std.crypto.hash.sha2.Sha256.init(.{});
                var buf: [4096]u8 = undefined;
                while (true) {
                    const n = try file.read(&buf);
                    if (n == 0) break;
                    hasher.update(buf[0..n]);
                }

                var hash: [32]u8 = undefined;
                hasher.final(&hash);

                // Format: "path hash"
                var hash_hex: [64]u8 = undefined;
                for (hash, 0..) |byte, i| {
                    _ = try std.fmt.bufPrint(hash_hex[i * 2 ..][0..2], "{x:0>2}", .{byte});
                }
                const line = try std.fmt.allocPrint(allocator, "{s} {s}", .{ full_path, hash_hex });
                try lines.append(allocator, line);
            },
            .directory => {
                var subdir = try dir.openDir(entry.name, .{ .iterate = true });
                defer subdir.close();

                // Check if directory is empty
                var subiter = subdir.iterate();
                const has_entries = (try subiter.next()) != null;

                if (has_entries) {
                    // Recurse
                    try walkDirectory(allocator, subdir, full_path, lines);
                } else {
                    // Empty directory
                    const line = try std.fmt.allocPrint(allocator, "{s}/", .{full_path});
                    try lines.append(allocator, line);
                }
            },
            else => {},
        }
    }
}

pub fn basenameFromUri(url: []const u8) []const u8 {
    var i: usize = url.len;
    while (i > 0 and url[i - 1] != '/') : (i -= 1) {}
    return url[i..];
}

fn errExit(comptime fmt: []const u8, args: anytype) noreturn {
    std.log.err(fmt, args);
    std.process.exit(0xff);
}

fn stderrFile() std.fs.File {
    return if (zig15) std.fs.File.stderr() else std.io.getStdErr();
}

const zig15 = @import("builtin").zig_version.order(.{ .major = 0, .minor = 15, .patch = 0 }) != .lt;
const std15 = if (zig15) std else @import("std15");
const File15 = if (zig15) std.fs.File else std15.fs.File15;

const std = @import("std");
const ArrayList = if (zig15) std.ArrayList else std.ArrayListUnmanaged;
const LockFile = @import("LockFile.zig");
