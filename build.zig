const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zio_dep = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
    });
    const zio_mod = zio_dep.module("zio");

    const io_impl = b.option(enum {
        zio,
        std,
        single_threaded,
    }, "io", "Io implementation") orelse .zio;
    const options = b.addOptions();
    options.addOption(@TypeOf(io_impl), "io", io_impl);

    const exe = b.addExecutable(.{
        .name = "zio_flate_server",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "options", .module = options.createModule() },
                .{ .name = "zio", .module = zio_mod },
            },
            .link_libc = true,
        }),
    });
    b.installArtifact(exe);

    const benchmark = b.addExecutable(.{
        .name = "benchmark",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/benchmark.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zio", .module = zio_mod },
            },
        }),
    });
    b.installArtifact(benchmark);

    const run_step = b.step("run", "Run the server");
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());

    const bench_step = b.step("benchmark", "Run the benchmark client");
    const bench_cmd = b.addRunArtifact(benchmark);
    bench_step.dependOn(&bench_cmd.step);
    bench_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
        bench_cmd.addArgs(args);
    }
}
