pub fn main(init: std.process.Init.Minimal) !void {
    var arena_instance = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const arena = arena_instance.allocator();

    var threaded: std.Io.Threaded = .init(arena, .{
        .environ = init.environ,
        .argv0 = .init(init.args),
    });
    const io = threaded.io();

    var args_it = try init.args.iterateAllocator(arena);
    // no need to deinit, os will do it
    _ = args_it.skip();

    var args: [5][]const u8 = undefined;
    var arg_count: usize = 0;
    while (args_it.next()) |arg| : (arg_count += 1) {
        if (arg_count < args.len) args[arg_count] = arg;
    }
    if (arg_count != args.len) errExit("expected 5 cmdline args but got {}", .{arg_count});

    const cache_path = args[0];
    const scratch_path = args[1];
    const test_path = args[2];
    const output_path_src = args[3];
    const output_path_cache = args[4];

    // we commit the output to source control so that all platforms can just re-use
    // the output of msiexec for verification
    if (std.Io.Dir.cwd().access(io, output_path_src, .{})) {
        if (std.fs.path.dirname(output_path_cache)) |d| try std.Io.Dir.cwd().createDirPath(io, d);
        try std.Io.Dir.copyFile(.cwd(), output_path_src, .cwd(), output_path_cache, io, .{});
        return;
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => |e| return e,
    }

    const test_file: common.File = try .read(io, arena, test_path);
    defer test_file.deinit(arena);

    try test_file.fetchAll(io, arena, cache_path, scratch_path);

    var found_msi = false;

    var test_it = test_file.iterator();
    while (test_it.next()) |entry| {
        switch (entry.kind) {
            .cab => continue,
            .msi => {},
        }
        if (found_msi) @panic("should only be 1 msi file");
        found_msi = true;
        const basename = common.basenameFromUri(entry.uri);
        const installer_path = try std.fs.path.join(arena, &.{ scratch_path, "installer", basename });
        defer arena.free(installer_path);
        const install_path = try std.fs.path.join(arena, &.{ scratch_path, "install-msiexec" });
        defer arena.free(install_path);
        try msiexec(io, arena, cache_path, installer_path, install_path);

        // msiexec seems to copy the msi file for some reason?
        {
            const copied_msi = try std.fs.path.join(arena, &.{ install_path, basename });
            defer arena.free(copied_msi);
            std.log.info("deleting copied msi '{s}'", .{copied_msi});
            try std.Io.Dir.cwd().deleteFile(io, copied_msi);
        }

        {
            const tree = try common.allocTree(io, arena, install_path);
            defer arena.free(tree);
            for (&[_][]const u8{ output_path_src, output_path_cache }) |output_path| {
                try std.Io.Dir.cwd().createDirPath(io, std.fs.path.dirname(output_path).?);
                try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = output_path, .data = tree });
            }
        }
        try std.Io.Dir.cwd().deleteTree(io, scratch_path);
    }
}

fn msiexec(
    io: std.Io,
    scratch: std.mem.Allocator,
    lock_parent_path: []const u8,
    msi_file_path: []const u8,
    target_dir: []const u8,
) !void {
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
        common.flushRun(&stderr.interface, &argv) catch return stderr.err.?;
    }

    // msiexec only allows 1 installation on the entire system at a time!
    // (one of the problems with it and why this project exists)
    // so we use a lock file to ensure the zig build system doesn't attempt
    // to execute multiple at a time which would cause the build to fail.
    const lock_path = try std.fs.path.join(scratch, &.{ lock_parent_path, "msiexec.lock" });
    defer scratch.free(lock_path);
    std.log.info("locking '{s}'...", .{lock_path});
    const lock_start = std.Io.Clock.awake.now(io);
    var lock = try LockFile.lock(io, lock_path);
    defer lock.unlock(io);
    const lock_elapsed: u64 = @intCast(lock_start.untilNow(io, .awake).toNanoseconds());
    std.log.info("lock acquired after {} seconds", .{@as(f32, @floatFromInt(lock_elapsed)) / std.time.ns_per_s});

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

fn normalizePathSeps(path: []u8) void {
    for (path) |*char| char.* = switch (char.*) {
        '/' => '\\',
        else => |other| other,
    };
}

fn errExit(comptime fmt: []const u8, args: anytype) noreturn {
    std.log.err(fmt, args);
    std.process.exit(0xff);
}

const std = @import("std");
const common = @import("common.zig");
const LockFile = @import("LockFile.zig");
