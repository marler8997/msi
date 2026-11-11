pub const std_options: std.Options = .{
    // needs the std from zig-staticio, see build.zig
    .io_vtable = .threaded,
    .logFn = log,
};

/// Without colors, std's default sets them at every one of the many places that log, which adds
/// tens of KiB to the exe.
fn log(
    comptime level: std.log.Level,
    comptime scope: @EnumLiteral(),
    comptime format: []const u8,
    args: anytype,
) void {
    const prefix = comptime level.asText() ++ if (scope == .default) ": " else "(" ++ @tagName(scope) ++ "): ";
    var buffer: [64]u8 = undefined;
    const stderr = std.debug.lockStderr(&buffer);
    defer std.debug.unlockStderr();
    stderr.file_writer.interface.print(prefix ++ format ++ "\n", args) catch {};
}

pub fn main(init: std.process.Init.Minimal) !void {
    var arena_instance = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const arena = arena_instance.allocator();

    var threaded: std.Io.Threaded = .init(arena, .{
        .environ = init.environ,
        .argv0 = .init(init.args),
    });
    const io = threaded.io();

    var opt: struct {
        verify: bool = false,
    } = .{};

    var args_it = try init.args.iterateAllocator(arena);
    // no need to deinit, os will do it
    _ = args_it.skip();

    var non_option_args: [2][]const u8 = undefined;
    var non_option_count: usize = 0;
    while (args_it.next()) |arg| {
        if (!std.mem.startsWith(u8, arg, "-")) {
            if (non_option_count < non_option_args.len) non_option_args[non_option_count] = arg;
            non_option_count += 1;
        } else if (std.mem.eql(u8, arg, "--verify")) {
            opt.verify = true;
        } else {
            errExit("unknown cmdline option '{s}'", .{arg});
        }
    }
    if (non_option_count != non_option_args.len) {
        std.log.err("expected 2 cmdline args (MSI/DIR) but but {}", .{non_option_count});
        std.process.exit(0xff);
    }
    const msi_path = non_option_args[0];
    const install_path = non_option_args[1];

    {
        const start = std.Io.Clock.awake.now(io);
        const msi_content = blk: {
            const msi_file = std.Io.Dir.cwd().openFile(io, msi_path, .{ .allow_directory = false }) catch |err| errExit(
                "open '{s}' failed with {t}",
                .{ msi_path, err },
            );
            defer msi_file.close(io);
            var msi_reader = msi_file.reader(io, &.{});
            break :blk msi_reader.interface.allocRemaining(std.heap.page_allocator, .unlimited) catch |err| switch (err) {
                error.ReadFailed => errExit("read '{s}' failed with {t}", .{ msi_path, msi_reader.err.? }),
                error.OutOfMemory => |e| return e,
                error.StreamTooLong => unreachable,
            };
        };
        defer std.heap.page_allocator.free(msi_content);
        try msi.install(io, arena, .{
            .cabs_dir = std.fs.path.dirname(msi_path) orelse ".",
            .msi_content = msi_content,
            .install_path = install_path,
            .big_allocator = std.heap.page_allocator,
        });
        const elapsed: u64 = @intCast(start.untilNow(io, .awake).toNanoseconds());
        std.log.info("installed msi in {d:.3} seconds", .{@as(f32, @floatFromInt(elapsed)) / std.time.ns_per_s});
    }

    if (opt.verify) {
        std.log.info("verifying...", .{});
        const verify_path = try std.mem.concat(arena, u8, &.{ install_path, ".verify" });
        defer arena.free(verify_path);
        try std.Io.Dir.cwd().deleteTree(io, verify_path);

        const start = std.Io.Clock.awake.now(io);
        try msiexec(io, arena, msi_path, verify_path);
        const elapsed: u64 = @intCast(start.untilNow(io, .awake).toNanoseconds());
        std.log.info("msiexec took {d:.3} seconds", .{@as(f32, @floatFromInt(elapsed)) / std.time.ns_per_s});

        // msiexec seems to copy the msi file for some reason?
        {
            const copied_msi = try std.fs.path.join(arena, &.{ verify_path, std.fs.path.basename(msi_path) });
            defer arena.free(copied_msi);
            std.log.info("deleting copied msi '{s}'", .{copied_msi});
            try std.Io.Dir.cwd().deleteFile(io, copied_msi);
        }

        const expected = try std.Io.Dir.cwd().openDir(io, verify_path, .{ .iterate = true });
        defer expected.close(io);
        const actual = try std.Io.Dir.cwd().openDir(io, install_path, .{ .iterate = true });
        defer actual.close(io);
        switch (try diffDirs(io, null, expected, actual)) {
            .identical => {
                std.log.info("verified outputs match", .{});
                try std.Io.Dir.cwd().deleteTree(io, verify_path);
            },
            .different => errExit("our msi installer differs from the official one (see differences above)", .{}),
        }
    }
}

fn msiexec(io: std.Io, scratch: std.mem.Allocator, msi_file_path: []const u8, target_dir: []const u8) !void {
    // msiexec has problem with paths that contain forward slaces
    const msi_path_fixed = try scratch.dupe(u8, msi_file_path);
    defer scratch.free(msi_path_fixed);
    normalizePathSeps(msi_path_fixed);

    // at least some MSI files require TARGETDIR to be an absolute path
    // also, realpath requires the directory to exist to work
    try std.Io.Dir.cwd().createDirPath(io, target_dir);
    const target_dir_abs = try std.Io.Dir.cwd().realPathFileAlloc(io, target_dir, scratch);
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
        var stderr = std.Io.File.stderr().writerStreaming(io, &stderr_buf);
        flushRun(&stderr.interface, &argv) catch return stderr.err.?;
    }

    const result = try std.process.run(scratch, io, .{ .argv = &argv });
    defer {
        scratch.free(result.stdout);
        scratch.free(result.stderr);
    }
    switch (result.term) {
        .exited => |exit_code| {
            if (exit_code != 0) {
                var stderr = std.Io.File.stderr().writerStreaming(io, &.{});
                stderr.interface.writeAll(result.stdout) catch return stderr.err.?;
                stderr.interface.writeAll(result.stderr) catch return stderr.err.?;
                errExit(
                    "msiexec for '{s}' failed with exit code {} (output stdout={} bytes stderr={} bytes)",
                    .{ msi_file_path, exit_code, result.stdout.len, result.stderr.len },
                );
            }
        },
        else => {
            var stderr = std.Io.File.stderr().writerStreaming(io, &.{});
            stderr.interface.writeAll(result.stdout) catch return stderr.err.?;
            stderr.interface.writeAll(result.stderr) catch return stderr.err.?;
            errExit("msiexec for '{s}' terminated with {any}", .{ msi_file_path, result.term });
        },
    }
}

fn flushRun(writer: *std.Io.Writer, cli: []const []const u8) error{WriteFailed}!void {
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
    pub fn format(p: PathNode, writer: *std.Io.Writer) error{WriteFailed}!void {
        if (p.parent) |parent| {
            try writer.print("{f}{s}", .{ parent, std.fs.path.sep_str });
        }
        try writer.writeAll(p.name);
    }
};
const Diff = enum { identical, different };
fn diffDirs(io: std.Io, parent: ?*const PathNode, expected: std.Io.Dir, actual: std.Io.Dir) !Diff {
    var result: Diff = .identical;

    // Pass 1: check all entries in expected exist and match in actual
    var exp_iter = expected.iterate();
    while (try exp_iter.next(io)) |entry| {
        const path: PathNode = .{ .parent = parent, .name = entry.name };
        switch (entry.kind) {
            .file => {
                const exp_file = try expected.openFile(io, path.name, .{});
                defer exp_file.close(io);
                const act_file = actual.openFile(io, path.name, .{}) catch |err| switch (err) {
                    error.FileNotFound => {
                        std.log.err("expected file not found in actual: '{f}'", .{path});
                        result = .different;
                        continue;
                    },
                    else => |e| return e,
                };
                defer act_file.close(io);
                if (!try fileContentsEqual(io, exp_file, act_file)) {
                    std.log.err("file contents differ: '{f}'", .{path});
                    result = .different;
                }
            },
            .directory => {
                const exp_sub = try expected.openDir(io, path.name, .{ .iterate = true });
                defer exp_sub.close(io);
                const act_sub = actual.openDir(io, path.name, .{ .iterate = true }) catch |err| switch (err) {
                    error.FileNotFound => {
                        std.log.err("expected directory not found in actual: '{f}'", .{path});
                        result = .different;
                        continue;
                    },
                    else => |e| return e,
                };
                defer act_sub.close(io);
                if (try diffDirs(io, &path, exp_sub, act_sub) == .different) {
                    result = .different;
                }
            },
            else => {},
        }
    }

    // Pass 2: find entries only in actual (not in expected)
    var act_iter = actual.iterate();
    while (try act_iter.next(io)) |entry| {
        const name = entry.name;
        const found_in_expected = switch (entry.kind) {
            .file => blk: {
                const f = expected.openFile(io, name, .{}) catch |err| switch (err) {
                    error.FileNotFound => break :blk false,
                    else => |e| return e,
                };
                f.close(io);
                break :blk true;
            },
            .directory => blk: {
                const d = expected.openDir(io, name, .{}) catch |err| switch (err) {
                    error.FileNotFound => break :blk false,
                    else => |e| return e,
                };
                d.close(io);
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

fn fileContentsEqual(io: std.Io, a: std.Io.File, b: std.Io.File) !bool {
    if (try a.length(io) != try b.length(io)) return false;

    var buf_a: [4096]u8 = undefined;
    var buf_b: [4096]u8 = undefined;
    var offset: u64 = 0;
    while (true) {
        const n_a = try a.readPositionalAll(io, &buf_a, offset);
        const n_b = try b.readPositionalAll(io, &buf_b, offset);
        if (n_a != n_b or !std.mem.eql(u8, buf_a[0..n_a], buf_b[0..n_b])) return false;
        if (n_a == 0) return true;
        offset += n_a;
    }
}

fn errExit(comptime fmt: []const u8, args: anytype) noreturn {
    std.log.err(fmt, args);
    std.process.exit(0xff);
}

const std = @import("std");
const msi = @import("msi");
