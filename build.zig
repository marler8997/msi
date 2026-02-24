pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const vers = b.createModule(.{
        .root_source_file = b.path(if (zig15) "vers/zig15.zig" else "vers/notzig15.zig"),
    });

    const stdfork = b.createModule(.{
        .root_source_file = b.path("stdfork/std.zig"),
        .imports = &.{
            .{ .name = "vers", .module = vers },
        },
    });
    if (!zig15) {
        if (b.lazyDependency("iobackport", .{})) |iobackport| {
            stdfork.addImport("std15", iobackport.module("std15"));
        }
    }

    const msi = b.addModule("msi", .{
        .root_source_file = b.path("msi/msi.zig"),
        .imports = &.{
            .{ .name = "vers", .module = vers },
            .{ .name = "stdfork", .module = stdfork },
        },
    });
    if (!zig15) {
        if (b.lazyDependency("iobackport", .{})) |iobackport| {
            msi.addImport("std15", iobackport.module("std15"));
        }
    }

    const msi_exe = b.addExecutable(.{
        .name = "msi",
        .root_module = b.createModule(.{
            .root_source_file = b.path("cli/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "msi", .module = msi }},
        }),
    });
    if (!zig15) {
        if (b.lazyDependency("iobackport", .{})) |iobackport| {
            msi_exe.root_module.addImport("std15", iobackport.module("std15"));
        }
    }

    const install_msi = b.addInstallArtifact(msi_exe, .{});
    b.getInstallStep().dependOn(&install_msi.step);

    {
        const run = b.addRunArtifact(msi_exe);
        run.step.dependOn(&install_msi.step);
        if (b.args) |a| run.addArgs(a);
        b.step("run", "").dependOn(&run.step);
    }

    const generate_verify_exe = b.addExecutable(.{
        .name = "generate-verify",
        .root_module = b.createModule(.{
            .root_source_file = b.path("test/generate-verify.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    if (!zig15) {
        if (b.lazyDependency("iobackport", .{})) |iobackport| {
            generate_verify_exe.root_module.addImport("std15", iobackport.module("std15"));
        }
    }

    const install_generate_verify = b.addInstallArtifact(generate_verify_exe, .{});

    const cache_path = b.pathFromRoot("cache");

    const test_step = b.step("test", "");
    const test_cases = [_][]const u8{
        "sdkheaders1",
        "sdkheaders2",
        "sdkheaders3",
        "sdkheaders4",
        "sdkheadersx64",
        "sdkheadersx86",
        "sdklibsarm",
        "sdklibsarm64",
        "sdklibsx64",
        "sdklibsx86",
        "sdkstoreheaders",
        "sdkstorelibs",
        "sdkstoretools",
        "sdksigningtools",
    };
    for (test_cases) |test_case| {
        const scratch_path = b.pathFromRoot(b.fmt("scratch/{s}", .{test_case}));

        const gen_verify = b.addRunArtifact(generate_verify_exe);
        gen_verify.step.name = b.fmt("gen verify {s}", .{test_case});
        gen_verify.step.dependOn(&install_generate_verify.step);
        gen_verify.addArg(cache_path);
        gen_verify.addArg(scratch_path);
        gen_verify.addFileArg(b.path(b.fmt("test/{s}", .{test_case})));
        gen_verify.addArg(b.pathFromRoot(b.fmt("test/verify/{s}", .{test_case})));
        const verify_file = gen_verify.addOutputFileArg(test_case);

        const exe = b.addExecutable(.{
            .name = "msitest",
            .root_module = b.createModule(.{
                .root_source_file = b.path("test/msitest.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        if (!zig15) {
            if (b.lazyDependency("iobackport", .{})) |iobackport| {
                exe.root_module.addImport("std15", iobackport.module("std15"));
            }
        }
        const install = b.addInstallArtifact(exe, .{});
        const run = b.addRunArtifact(exe);
        run.step.name = b.fmt("msitest {s}", .{test_case});
        run.expectExitCode(0);
        run.step.dependOn(&install.step);
        run.step.dependOn(&install_msi.step);
        run.addArtifactArg(msi_exe);
        run.addArg(cache_path);
        run.addArg(scratch_path);
        run.addFileArg(b.path(b.fmt("test/{s}", .{test_case})));
        run.addFileArg(verify_file);
        b.step(b.fmt("test-{s}", .{test_case}), "").dependOn(&run.step);
        test_step.dependOn(&run.step);
    }
}

const zig15 = @import("builtin").zig_version.order(.{ .major = 0, .minor = 15, .patch = 0 }) != .lt;

const std = @import("std");
