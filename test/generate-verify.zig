pub fn main() !void {
    var arena_instance = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const arena = arena_instance.allocator();
    const all_args = try std.process.argsAlloc(arena);
    // no need to free, os will do it

    const args = all_args[@min(all_args.len, 1)..];
    if (args.len != 5) errExit("expected 5 cmdline args but got {}", .{args.len});

    const cache_path = args[0];
    const scratch_path = args[1];
    const test_path = args[2];
    const output_path_src = args[3];
    const output_path_cache = args[4];

    // we commit the output to source control so that all platforms can just re-use
    // the output of msiexec for verification
    if (std.fs.cwd().access(output_path_src, .{})) {
        if (std.fs.path.dirname(output_path_cache)) |d| try std.fs.cwd().makePath(d);
        try std.fs.cwd().copyFile(output_path_src, std.fs.cwd(), output_path_cache, .{});
        return;
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => |e| return e,
    }

    const test_file: common.File = try .read(arena, test_path);
    defer test_file.deinit(arena);

    try test_file.fetchAll(arena, cache_path, scratch_path);

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
        try msiexec(arena, cache_path, installer_path, install_path);

        // msiexec seems to copy the msi file for some reason?
        {
            const copied_msi = try std.fs.path.join(arena, &.{ install_path, basename });
            defer arena.free(copied_msi);
            std.log.info("deleting copied msi '{s}'", .{copied_msi});
            try std.fs.cwd().deleteFile(copied_msi);
        }

        {
            const tree = try common.allocTree(arena, install_path);
            defer arena.free(tree);
            for (&[_][]const u8{ output_path_src, output_path_cache }) |output_path| {
                try std.fs.cwd().makePath(std.fs.path.dirname(output_path).?);
                const output_file = try std.fs.cwd().createFile(output_path, .{});
                defer output_file.close();
                try output_file.writeAll(tree);
            }
        }
        try std.fs.cwd().deleteTree(scratch_path);
    }
}

// TODO: implement this at the end
fn acquireGlobalLock(allocator: std.mem.Allocator) !std.fs.File {
    const lock_path = try std.fs.getAppDataDir(allocator, "msiexecwrapper.lock");
    defer allocator.free(lock_path);

    if (std.fs.path.dirname(lock_path)) |dir| {
        try std.fs.cwd().makePath(dir);
    }

    // Try to create lock file exclusively
    while (true) {
        const lock_file = std.fs.cwd().createFile(lock_path, .{ .exclusive = true }) catch |err| switch (err) {
            error.PathAlreadyExists => {
                // Lock held by another process, wait and retry
                std.Thread.sleep(100 * std.time.ns_per_ms);
                continue;
            },
            else => return err,
        };
        return lock_file;
    }
}

fn releaseLock(lock_file: std.fs.File) void {
    lock_file.close();
    // Note: we don't delete the lock file as close() should release it on Windows
}

fn msiexec(
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
        common.flushRun(&stderr.interface, &argv) catch return stderr.err.?;
    }

    // msiexec only allows 1 installation on the entire system at a time!
    // (one of the problems with it and why this project exists)
    // so we use a lock file to ensure the zig build system doesn't attempt
    // to execute multiple at a time which would cause the build to fail.
    const lock_path = try std.fs.path.join(scratch, &.{ lock_parent_path, "msiexec.lock" });
    defer scratch.free(lock_path);
    std.log.info("locking '{s}'...", .{lock_path});
    const lock_start = try std.time.Instant.now();
    var lock = try LockFile.lock(lock_path);
    defer lock.unlock();
    const lock_elapsed = (try std.time.Instant.now()).since(lock_start);
    std.log.info("lock acquired after {} seconds", .{@as(f32, @floatFromInt(lock_elapsed)) / std.time.ns_per_s});

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

fn stderrFile() std.fs.File {
    return if (zig15) std.fs.File.stderr() else std.io.getStdErr();
}

const zig15 = @import("builtin").zig_version.order(.{ .major = 0, .minor = 15, .patch = 0 }) != .lt;
const std15 = if (zig15) std else @import("std15");
const File15 = if (zig15) std.fs.File else std15.fs.File15;

const std = @import("std");
const common = @import("common.zig");
const LockFile = @import("LockFile.zig");
