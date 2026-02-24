pub fn main() !void {
    var arena_instance = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const arena = arena_instance.allocator();

    var opt: struct {
        verify: bool = false,
    } = .{};

    const all_args = try std.process.argsAlloc(arena);
    // no need to free, os will do it

    const non_option_args = blk: {
        var non_option_count: usize = 0;
        var i: usize = 1;
        while (i < all_args.len) : (i += 1) {
            const arg = all_args[i];
            if (!std.mem.startsWith(u8, arg, "-")) {
                all_args[non_option_count] = arg;
                non_option_count += 1;
            } else if (std.mem.eql(u8, arg, "--verify")) {
                opt.verify = true;
            } else {
                errExit("unknown cmdline option '{s}'", .{arg});
            }
        }
        break :blk all_args[0..non_option_count];
    };
    if (non_option_args.len != 2) {
        std.log.err("expected 2 cmdline args (MSI/DIR) but but {}", .{non_option_args.len});
        std.process.exit(0xff);
    }
    const msi_path = non_option_args[0];
    const install_path = non_option_args[1];

    {
        const start = try std.time.Instant.now();
        const msi_content = blk: {
            const msi_file = std.fs.cwd().openFile(msi_path, .{}) catch |err| errExit(
                "open '{s}' failed with {s}",
                .{ msi_path, @errorName(err) },
            );
            defer msi_file.close();
            break :blk try msi_file.readToEndAlloc(arena, std.math.maxInt(usize));
        };
        defer arena.free(msi_content);
        try msi.install(arena, .{
            .cabs_dir = std.fs.path.dirname(msi_path) orelse ".",
            .msi_content = msi_content,
            .install_path = install_path,
        });
        const elapsed = (try std.time.Instant.now()).since(start);
        std.log.info("installed msi in {d:.3} seconds", .{@as(f32, @floatFromInt(elapsed)) / std.time.ns_per_s});
    }

    if (opt.verify) {
        std.log.info("verifying...", .{});
        const verify_path = try std.mem.concat(arena, u8, &.{ install_path, ".verify" });
        defer arena.free(verify_path);
        try std.fs.cwd().deleteTree(verify_path);

        const start = try std.time.Instant.now();
        try msiexec(arena, msi_path, verify_path);
        const elapsed = (try std.time.Instant.now()).since(start);
        std.log.info("msiexec took {d:.3} seconds", .{@as(f32, @floatFromInt(elapsed)) / std.time.ns_per_s});

        // msiexec seems to copy the msi file for some reason?
        {
            const copied_msi = try std.fs.path.join(arena, &.{ verify_path, std.fs.path.basename(msi_path) });
            defer arena.free(copied_msi);
            std.log.info("deleting copied msi '{s}'", .{copied_msi});
            try std.fs.cwd().deleteFile(copied_msi);
        }

        var expected = try std.fs.cwd().openDir(verify_path, .{ .iterate = true });
        defer expected.close();
        var actual = try std.fs.cwd().openDir(install_path, .{ .iterate = true });
        defer actual.close();
        switch (try diffDirs(null, expected, actual)) {
            .identical => {
                std.log.info("verified outputs match", .{});
                try std.fs.cwd().deleteTree(verify_path);
            },
            .different => errExit("our msi installer differs from the official one (see differences above)", .{}),
        }
    }
}

fn msiexec(scratch: std.mem.Allocator, msi_file_path: []const u8, target_dir: []const u8) !void {
    // msiexec has problem with paths that contain forward slaces
    const msi_path_fixed = try scratch.dupe(u8, msi_file_path);
    defer scratch.free(msi_path_fixed);
    normalizePathSeps(msi_path_fixed);

    // at least some MSI files require TARGETDIR to be an absolute path
    // also, realpath requires the directory to exist to work
    try std.fs.cwd().makePath(target_dir);
    const target_dir_abs = try std.fs.cwd().realpathAlloc(scratch, target_dir);
    defer scratch.free(target_dir_abs);
    normalizePathSeps(target_dir_abs);

    const target_dir_arg = try std.mem.concat(scratch, u8, &.{ "TARGETDIR=", target_dir_abs });
    defer scratch.free(target_dir_arg);
    const argv = [_][]const u8{
        "msiexec.exe",
        "/a",
        msi_path_fixed,
        "/quiet",
        "/qn",
        //"/?",
        //"/lv", "C:\\temp\\log.txt",
        target_dir_arg,
    };

    {
        var stderr_buf: [1000]u8 = undefined;
        var stderr: File15.Writer = .init(stderrFile(), &stderr_buf);
        flushRun(&stderr.interface, &argv) catch return stderr.err.?;
    }

    const result = try std.process.Child.run(.{
        .allocator = scratch,
        .argv = &argv,
    });
    defer {
        scratch.free(result.stdout);
        scratch.free(result.stderr);
    }
    switch (result.term) {
        .Exited => |exit_code| {
            if (exit_code != 0) {
                var stderr: File15.Writer = .init(stderrFile(), &.{});
                stderr.interface.writeAll(result.stdout) catch return stderr.err.?;
                stderr.interface.writeAll(result.stderr) catch return stderr.err.?;
                errExit(
                    "msiexec for '{s}' failed with exit code {} (output stdout={} bytes stderr={} bytes)",
                    .{ msi_file_path, exit_code, result.stdout.len, result.stderr.len },
                );
            }
        },
        inline else => |e| {
            var stderr: File15.Writer = .init(stderrFile(), &.{});
            stderr.interface.writeAll(result.stdout) catch return stderr.err.?;
            stderr.interface.writeAll(result.stderr) catch return stderr.err.?;
            errExit("msiexec for '{s}' terminated with {}", .{ msi_file_path, e });
        },
    }
}

fn flushRun(writer: *std15.Io.Writer, cli: []const []const u8) error{WriteFailed}!void {
    try writer.writeAll("run:");
    for (cli) |a| {
        const do_quote = std.mem.indexOfScalar(u8, a, ' ') != null;
        const quote: []const u8 = if (do_quote) "\"" else "";
        try writer.print(" {s}{s}{0s}", .{ quote, a });
    }
    try writer.writeAll("\n");
    try writer.flush();
}

fn normalizePathSeps(path: []u8) void {
    for (path) |*char| char.* = switch (char.*) {
        '/' => '\\',
        else => |other| other,
    };
}

const PathNode = struct {
    parent: ?*const PathNode,
    name: []const u8,
    pub const format = if (zig15) formatNew else formatOld;
    pub fn formatNew(p: PathNode, writer: *std15.Io.Writer) error{WriteFailed}!void {
        try formatOld(p, "", .{}, writer);
    }
    pub fn formatOld(p: PathNode, comptime fmt: []const u8, opt: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = opt;
        if (p.parent) |parent| {
            try writer.print("{f}{s}", .{ parent, std.fs.path.sep_str });
        }
        try writer.writeAll(p.name);
    }
};
const Diff = enum { identical, different };
fn diffDirs(parent: ?*const PathNode, expected: std.fs.Dir, actual: std.fs.Dir) !Diff {
    var result: Diff = .identical;

    // Pass 1: check all entries in expected exist and match in actual
    var exp_iter = expected.iterate();
    while (try exp_iter.next()) |entry| {
        const path: PathNode = .{ .parent = parent, .name = entry.name };
        switch (entry.kind) {
            .file => {
                const exp_file = try expected.openFile(path.name, .{});
                defer exp_file.close();
                const act_file = actual.openFile(path.name, .{}) catch |err| switch (err) {
                    error.FileNotFound => {
                        std.log.err("expected file not found in actual: '{f}'", .{path});
                        result = .different;
                        continue;
                    },
                    else => |e| return e,
                };
                defer act_file.close();
                if (!try fileContentsEqual(exp_file, act_file)) {
                    std.log.err("file contents differ: '{f}'", .{path});
                    result = .different;
                }
            },
            .directory => {
                var exp_sub = try expected.openDir(path.name, .{ .iterate = true });
                defer exp_sub.close();
                var act_sub = actual.openDir(path.name, .{ .iterate = true }) catch |err| switch (err) {
                    error.FileNotFound => {
                        std.log.err("expected directory not found in actual: '{f}'", .{path});
                        result = .different;
                        continue;
                    },
                    else => |e| return e,
                };
                defer act_sub.close();
                if (try diffDirs(&path, exp_sub, act_sub) == .different) {
                    result = .different;
                }
            },
            else => {},
        }
    }

    // Pass 2: find entries only in actual (not in expected)
    var act_iter = actual.iterate();
    while (try act_iter.next()) |entry| {
        const name = entry.name;
        const found_in_expected = switch (entry.kind) {
            .file => blk: {
                const f = expected.openFile(name, .{}) catch |err| switch (err) {
                    error.FileNotFound => break :blk false,
                    else => |e| return e,
                };
                f.close();
                break :blk true;
            },
            .directory => blk: {
                var d = expected.openDir(name, .{}) catch |err| switch (err) {
                    error.FileNotFound => break :blk false,
                    else => |e| return e,
                };
                d.close();
                break :blk true;
            },
            else => continue,
        };
        if (!found_in_expected) {
            std.log.err("unexpected entry in actual: '{s}'", .{name});
            result = .different;
        }
    }

    return result;
}

fn fileContentsEqual(a: std.fs.File, b: std.fs.File) !bool {
    const stat_a = try a.stat();
    const stat_b = try b.stat();
    if (stat_a.size != stat_b.size) return false;

    var buf_a: [4096]u8 = undefined;
    var buf_b: [4096]u8 = undefined;
    while (true) {
        const n_a = try a.read(&buf_a);
        const n_b = try b.read(&buf_b);
        if (n_a != n_b or !std.mem.eql(u8, buf_a[0..n_a], buf_b[0..n_b])) return false;
        if (n_a == 0) return true;
    }
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
const msi = @import("msi");
