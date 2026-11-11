pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const msi = b.addModule("msi", .{
        .root_source_file = b.path("msi/msi.zig"),
    });

    const patched_std = b.dependency("patch", .{
        .std_patches = &[_]std.Build.LazyPath{
            // std.Io with static dispatch, so the exe only has the Io operations it can reach
            b.dependency("staticio", .{}).namedLazyPath("std.patch"),
            // takes 192KiB of constants out of a windows exe
            b.path("cli/std-windows-pathspace.patch"),
        },
    }).module("std");

    const msi_exe = b.addExecutable(.{
        .name = "msi",
        .root_module = b.createModule(.{
            .root_source_file = b.path("cli/main.zig"),
            .target = target,
            .optimize = optimize,
            // leaves out the locking in std
            .single_threaded = true,
            .imports = &.{
                .{ .name = "msi", .module = msi },
                .{ .name = "std", .module = patched_std },
            },
        }),
    });

    const install_msi = b.addInstallArtifact(msi_exe, .{});
    b.getInstallStep().dependOn(&install_msi.step);

    {
        const run = b.addRunArtifact(msi_exe);
        run.step.dependOn(&install_msi.step);
        run.addPassthruArgs();
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

    const install_generate_verify = b.addInstallArtifact(generate_verify_exe, .{});

    const msitest_exe = b.addExecutable(.{
        .name = "msitest",
        .root_module = b.createModule(.{
            .root_source_file = b.path("test/msitest.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const install_msitest = b.addInstallArtifact(msitest_exe, .{});

    const cache_path = b.path("cache");

    const test_step = b.step("test", "");
    test_step.dependOn(&b.addRunArtifact(b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("msi/msi.zig"),
            .target = target,
            .optimize = optimize,
        }),
    })).step);
    const test_cases = [_][]const u8{
        "sdkheaders1",
        "sdkheaders2",
        "sdkheaders3",
        "sdkheaders4",
        "sdkheaders5",
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
        const scratch_path = b.path(b.fmt("scratch/{s}", .{test_case}));

        const gen_verify = b.addRunArtifact(generate_verify_exe);
        gen_verify.step.name = b.fmt("gen verify {s}", .{test_case});
        gen_verify.step.dependOn(&install_generate_verify.step);
        gen_verify.addDirectoryArg2(cache_path, .{});
        gen_verify.addDirectoryArg2(scratch_path, .{});
        gen_verify.addFileArg(b.path(b.fmt("test/{s}", .{test_case})));
        gen_verify.addDirectoryArg2(b.path("test/verify"), .{ .suffix = b.fmt("/{s}", .{test_case}) });
        const verify_file = gen_verify.addOutputFileArg(test_case);

        const run = b.addRunArtifact(msitest_exe);
        run.step.name = b.fmt("msitest {s}", .{test_case});
        run.expectExitCode(0);
        run.step.dependOn(&install_msitest.step);
        run.step.dependOn(&install_msi.step);
        run.addArtifactArg(msi_exe);
        run.addDirectoryArg2(cache_path, .{});
        run.addDirectoryArg2(scratch_path, .{});
        run.addFileArg(b.path(b.fmt("test/{s}", .{test_case})));
        run.addFileArg(verify_file);
        b.step(b.fmt("test-{s}", .{test_case}), "").dependOn(&run.step);
        test_step.dependOn(&run.step);
    }
}

const std = @import("std");
