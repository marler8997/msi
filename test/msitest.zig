pub fn main() !void {
    var arena_instance = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const arena = arena_instance.allocator();
    const all_args = try std.process.argsAlloc(arena);
    // no need to free, os will do it

    const args = all_args[@min(all_args.len, 1)..];
    if (args.len != 5) errExit("expected 5 cmdline args but got {}", .{args.len});

    const msi_exe = args[0];
    const cache_path = args[1];
    const scratch_path = args[2];
    const test_path = args[3];
    const verify_path = args[4];

    const verify_tree = try std.fs.cwd().readFileAlloc(arena, verify_path, std.math.maxInt(usize));

    try std.fs.cwd().deleteTree(scratch_path);

    const test_file: common.File = try .read(arena, test_path);
    defer test_file.deinit(arena);

    try test_file.fetchAll(arena, cache_path, scratch_path);

    var test_it = test_file.iterator();
    while (test_it.next()) |entry| {
        switch (entry.kind) {
            .cab => continue,
            .msi => {},
        }
        const basename = common.basenameFromUri(entry.uri);
        const installer_path = try std.fs.path.join(arena, &.{ scratch_path, "installer", basename });
        defer arena.free(installer_path);
        const install_path = try std.fs.path.join(arena, &.{ scratch_path, "install" });
        defer arena.free(install_path);
        const argv = [_][]const u8{ msi_exe, installer_path, install_path } ++ switch (builtin.os.tag) {
            // .windows => [_][]const u8{"--verify"},
            else => [_][]const u8{},
        };
        {
            var stderr_buf: [1000]u8 = undefined;
            var stderr: File15.Writer = .init(stderrFile(), &stderr_buf);
            common.flushRun(&stderr.interface, &argv) catch return stderr.err.?;
        }
        var proc_arena = std.heap.ArenaAllocator.init(arena);
        defer proc_arena.deinit();
        const result = try std.process.Child.run(.{
            .allocator = proc_arena.allocator(),
            .argv = &argv,
        });
        {
            var stdout: File15.Writer = .init(stdoutFile(), &.{});
            stdout.interface.writeAll(result.stdout) catch return stdout.err.?;
        }
        {
            var stderr: File15.Writer = .init(stderrFile(), &.{});
            stderr.interface.writeAll(result.stderr) catch return stderr.err.?;
        }
        switch (result.term) {
            .Exited => |code| if (code != 0) errExit("msi exited with code {}", .{code}),
            inline else => |sig, result_kind| errExit("msi stopped ({s}) with {}", .{ @tagName(result_kind), sig }),
        }

        const our_tree = try common.allocTree(arena, install_path);
        defer arena.free(our_tree);
        if (!std.mem.eql(u8, verify_tree, our_tree)) {
            var stderr_buf: [2000]u8 = undefined;
            var stderr: File15.Writer = .init(stderrFile(), &stderr_buf);
            const w = &stderr.interface;
            writeMismatch(w, verify_tree, our_tree) catch return stderr.err.?;
            writeDiff(w, verify_tree, our_tree) catch return stderr.err.?;
            errExit("our output does not match msiexec", .{});
        }

        const keep_installer = true;
        if (keep_installer) {
            try std.fs.cwd().deleteTree(install_path);
        } else {
            try std.fs.cwd().deleteTree(scratch_path);
        }
    }
}

fn countLines(tree: []const u8) u32 {
    var line_count: u32 = 0;
    var it = std.mem.splitScalar(u8, tree, '\n');
    while (it.next()) |_| {
        line_count += 1;
    }
    return line_count;
}

fn writeMismatch(
    writer: *std15.Io.Writer,
    msiexec_tree: []const u8,
    our_tree: []const u8,
) error{WriteFailed}!void {
    try writer.writeAll("error: our output does not match msiexec!\n");
    try writer.print("--- msiexec tree {} entries ---\n", .{countLines(msiexec_tree)});
    try writer.writeAll(msiexec_tree);
    try writer.print("--- our tree {} entries ---\n", .{countLines(our_tree)});
    try writer.writeAll(our_tree);
    try writer.flush();
}

fn writeDiff(
    writer: *std15.Io.Writer,
    msiexec_tree: []const u8,
    our_tree: []const u8,
) error{WriteFailed}!void {
    try writer.writeAll("--- DIFF ---\n");
    var msi_it = std.mem.splitScalar(u8, msiexec_tree, '\n');
    var our_it = std.mem.splitScalar(u8, our_tree, '\n');
    while (true) {
        const msi_line = msi_it.next() orelse {
            while (our_it.next()) |line| {
                try writer.print("> {s}\n", .{line});
            }
            break;
        };
        const our_line = our_it.next() orelse {
            var maybe_line: ?[]const u8 = msi_line;
            while (maybe_line) |line| {
                try writer.print("< {s}\n", .{line});
                maybe_line = msi_it.next();
            }
            break;
        };
        if (std.mem.eql(u8, msi_line, our_line)) continue;
    }
    try writer.flush();
}

fn errExit(comptime fmt: []const u8, args: anytype) noreturn {
    std.log.err(fmt, args);
    std.process.exit(0xff);
}

fn stdoutFile() std.fs.File {
    return if (zig15) std.fs.File.stdout() else std.io.getStdOut();
}
fn stderrFile() std.fs.File {
    return if (zig15) std.fs.File.stderr() else std.io.getStdErr();
}

const zig15 = @import("builtin").zig_version.order(.{ .major = 0, .minor = 15, .patch = 0 }) != .lt;
const std15 = if (zig15) std else @import("std15");
const File15 = if (zig15) std.fs.File else std15.fs.File15;

const builtin = @import("builtin");
const std = @import("std");
const common = @import("common.zig");
